# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Silver to Gold (star schema)
# MAGIC Builds the business-ready layer from Silver:
# MAGIC
# MAGIC | Table | Role | Refresh strategy |
# MAGIC |---|---|---|
# MAGIC | `dim_time` | Date dimension | Overwrite (tiny, rebuilt from a date sequence) |
# MAGIC | `dim_location`, `dim_agency`, `dim_complaint_type` | Descriptive dimensions | `MERGE INTO` (insert new members, update changed attributes) |
# MAGIC | `fact_311_requests` | One row per request | `MERGE INTO` on `unique_key`, only for Silver rows newer than the Gold watermark |
# MAGIC | `agg_daily_requests` | Pre-aggregated table for fast dashboards | Overwrite |
# MAGIC
# MAGIC Every table is logged to `pipeline_execution_logs` with layer `Silver-to-Gold`.
# MAGIC Run it through `04_run_pipeline` so Gold refreshes right after Silver, or on its own for a rebuild
# MAGIC (`full_refresh=true`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

full_refresh = get_param("full_refresh", "false").lower() == "true"
run_label = safe_id(get_param("run_label", "") or "GOLD_" + utc_now().strftime("%Y%m%d%H%M%S"))
load_label = "FULL_REFRESH" if full_refresh else "INCREMENTAL"
params = dict(full_refresh=full_refresh, run_label=run_label)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the tables (explicit DDL). The fact table uses liquid clustering.

# COMMAND ----------

spark.sql(gold_ddl(GOLD_DIM_TIME_TBL, DIM_TIME_COLUMNS, comment="Date dimension, one row per day"))
spark.sql(gold_ddl(GOLD_DIM_LOCATION_TBL, DIM_LOCATION_COLUMNS, comment="Borough and zip code"))
spark.sql(gold_ddl(GOLD_DIM_AGENCY_TBL, DIM_AGENCY_COLUMNS, comment="Responding agency"))
spark.sql(gold_ddl(GOLD_DIM_TYPE_TBL, DIM_TYPE_COLUMNS, comment="Complaint type and group"))
spark.sql(gold_ddl(GOLD_FACT_TBL, FACT_COLUMNS, cluster_by=["created_date_key", "agency_key"],
                   comment="Fact table: one row per 311 service request"))
spark.sql(gold_ddl(GOLD_AGG_TBL, AGG_COLUMNS, comment="Daily counts by borough, agency and complaint type"))

# Informational primary and foreign keys (Unity Catalog does not enforce them; BI tools can read them).
constraints = [
    (GOLD_DIM_TIME_TBL, "date_key"), (GOLD_DIM_LOCATION_TBL, "location_key"),
    (GOLD_DIM_AGENCY_TBL, "agency_key"), (GOLD_DIM_TYPE_TBL, "complaint_type_key"),
    (GOLD_FACT_TBL, "unique_key"),
]
for table, key in constraints:
    try:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {key} SET NOT NULL")
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT pk_{table.split('.')[-1]} PRIMARY KEY ({key})")
    except Exception as e:
        pass   # already present, or not supported in this workspace
foreign_keys = [("created_date_key", GOLD_DIM_TIME_TBL, "date_key"), ("closed_date_key", GOLD_DIM_TIME_TBL, "date_key"),
                ("location_key", GOLD_DIM_LOCATION_TBL, "location_key"), ("agency_key", GOLD_DIM_AGENCY_TBL, "agency_key"),
                ("complaint_type_key", GOLD_DIM_TYPE_TBL, "complaint_type_key")]
for col, ref_table, ref_col in foreign_keys:
    try:
        spark.sql(f"ALTER TABLE {GOLD_FACT_TBL} ADD CONSTRAINT fk_fact_{col} "
                  f"FOREIGN KEY ({col}) REFERENCES {ref_table}({ref_col})")
    except Exception as e:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare the Silver data (normalise values and compute surrogate keys)

# COMMAND ----------

silver = spark.table(SILVER_TBL)
prep_all = gold_prepare(silver)


def merge_dim(df, target, key, attrs, view):
    """UPSERT a dimension: insert new members, update a member only when an attribute changed."""
    df.createOrReplaceTempView(view)
    changed = " OR ".join(f"NOT (t.{a} <=> s.{a})" for a in attrs)
    sets = ", ".join([f"t.{a} = s.{a}" for a in attrs] + ["t.load_timestamp = s.load_timestamp"])
    res = spark.sql(f"""
        MERGE INTO {target} t USING {view} s ON t.{key} = s.{key}
        WHEN MATCHED AND ({changed}) THEN UPDATE SET {sets}
        WHEN NOT MATCHED THEN INSERT *""").collect()[0].asDict()
    return int(res.get("num_inserted_rows") or 0), int(res.get("num_updated_rows") or 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_time (overwrite)

# COMMAND ----------

with PipelineRun("Silver-to-Gold", load_label, run_label, GOLD_DIM_TIME_TBL, params) as run:
    dim_time = build_dim_time(spark)
    dim_time.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIM_TIME_TBL)
    run.rows_read = run.rows_inserted = spark.table(GOLD_DIM_TIME_TBL).count()
    run.note("calendar 2020-01-01 to 2030-12-31, rebuilt by overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dimensions (MERGE). They are derived from all of Silver, which is small after grouping.

# COMMAND ----------

dim_jobs = [
    (GOLD_DIM_AGENCY_TBL, "agency_key", ["agency_code", "agency_name"], to_dim_agency, "src_dim_agency"),
    (GOLD_DIM_TYPE_TBL, "complaint_type_key", ["complaint_type", "complaint_group"], to_dim_type, "src_dim_type"),
    (GOLD_DIM_LOCATION_TBL, "location_key", ["borough", "zip_code", "centroid_latitude", "centroid_longitude"],
     to_dim_location, "src_dim_location"),
]
for target, key, attrs, builder, view in dim_jobs:
    with PipelineRun("Silver-to-Gold", load_label, run_label, target, params) as run:
        dim_df = builder(prep_all)
        run.rows_read = dim_df.count()
        run.rows_inserted, run.rows_updated = merge_dim(dim_df, target, key, attrs, view)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fact table (MERGE, incremental by Silver load_timestamp)
# MAGIC The Gold watermark is the newest Silver `load_timestamp` already processed. It moves forward only after the
# MAGIC MERGE succeeds, so a failed run is simply repeated next time.

# COMMAND ----------

with PipelineRun("Silver-to-Gold", load_label, run_label, GOLD_FACT_TBL, params) as run:
    watermark = None if full_refresh else read_control("gold_watermark")
    delta = prep_all if watermark is None else prep_all.where(F.col("silver_load_timestamp") > F.lit(watermark))
    run.rows_read = delta.count()
    run.note(f"gold watermark before run: {watermark}")
    if run.rows_read == 0:
        run.note("no Silver rows newer than the watermark; nothing to do")
    else:
        new_watermark = delta.agg(F.max("silver_load_timestamp")).collect()[0][0].replace(tzinfo=None)
        fact_df = to_fact(delta)
        if spark.table(GOLD_FACT_TBL).limit(1).count() == 0:
            fact_df.write.format("delta").mode("append").saveAsTable(GOLD_FACT_TBL)   # first load: plain insert
            run.rows_inserted = run.rows_read
        else:
            fact_df.createOrReplaceTempView("src_fact")
            res = spark.sql(f"""
                MERGE INTO {GOLD_FACT_TBL} t USING src_fact s ON t.unique_key = s.unique_key
                WHEN MATCHED AND t.source_row_hash <> s.source_row_hash THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *""").collect()[0].asDict()
            run.rows_inserted = int(res.get("num_inserted_rows") or 0)
            run.rows_updated = int(res.get("num_updated_rows") or 0)
        set_control("gold_watermark", new_watermark)
        run.note(f"gold watermark moved to {new_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Aggregate table (overwrite). This is the most complex transformation: a broadcast join and a group by.

# COMMAND ----------

with PipelineRun("Silver-to-Gold", load_label, run_label, GOLD_AGG_TBL, params) as run:
    fact = spark.table(GOLD_FACT_TBL)
    run.rows_read = fact.count()                      # fact rows aggregated
    agg = build_agg(fact, spark.table(GOLD_DIM_LOCATION_TBL))
    agg.write.format("delta").mode("overwrite").saveAsTable(GOLD_AGG_TBL)
    run.rows_inserted = spark.table(GOLD_AGG_TBL).count()
    run.note(f"aggregated {run.rows_read} fact rows into {run.rows_inserted} rows (rows_read = rows aggregated)")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"run_label": run_label, "full_refresh": full_refresh}))
