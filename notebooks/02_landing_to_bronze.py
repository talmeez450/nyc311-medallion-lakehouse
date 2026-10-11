# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Raw (landing files) to Bronze
# MAGIC * **Strict schema-on-read.** The JSON is read with the explicit `BRONZE_READ_SCHEMA` (StructType /
# MAGIC   StructField). Schema inference is never used.
# MAGIC * **Idempotent.** The batch is written with `replaceWhere batch_id = ...`, so running it again on the same
# MAGIC   files replaces the batch instead of duplicating it.
# MAGIC * **Schema drift.** Fields that do not fit the schema are kept in `_rescued_data` and never crash the job.
# MAGIC   With `schema_drift_mode = evolve` a new source column is added to the Bronze table and filled from
# MAGIC   `_rescued_data`.
# MAGIC * **Audit.** Every row gets `batch_id`, `load_type`, `source_file`, `extract_timestamp` and `load_timestamp`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

load_type         = get_param("load_type", "INCREMENTAL").upper()
batch_id          = safe_id(get_param("batch_id", ""))            # required
landing_path      = get_param("landing_path", "")                 # optional override, any folder or glob of JSON files
schema_drift_mode = get_param("schema_drift_mode", "evolve")      # evolve | log_only
purge_landing     = get_param("purge_landing", "false").lower() == "true"

# COMMAND ----------

batch_dir = f"{LANDING_ROOT}/{load_type.lower()}/{batch_id}"
read_path = landing_path or f"{batch_dir}/*/page_*.json"
manifest_path = f"{batch_dir}/_manifest.json"
params = dict(load_type=load_type, batch_id=batch_id, landing_path=landing_path,
              schema_drift_mode=schema_drift_mode, purge_landing=purge_landing)

with PipelineRun("Raw-to-Bronze", load_type, batch_id, read_path, params) as run:
    # 1. Fault tolerance: do not ingest a batch that was not fully landed.
    manifest = None
    if not landing_path:
        if not os.path.exists(manifest_path):
            raise FileNotFoundError(f"No _manifest.json in {batch_dir}. Run 01_api_to_landing first "
                                    "(an unfinished landing is never loaded).")
        manifest = json.load(open(manifest_path))
        if manifest.get("truncated"):
            run.note("WARNING: batch was truncated by max_pages (test run)")

    # 2. Strict schema-on-read with rescued-data column for drift.
    try:
        spark.conf.set("spark.databricks.sql.rescuedDataColumn.filePath.enabled", "false")
    except Exception:
        pass
    raw = (spark.read.format("json")
           .schema(BRONZE_READ_SCHEMA)
           .option("multiLine", "true")
           .option("mode", "PERMISSIVE")
           .option("rescuedDataColumn", "_rescued_data")
           .load(read_path))

    df = (raw
          .withColumn("source_file", F.col("_metadata.file_path"))
          .withColumn("extract_timestamp",
                      F.to_timestamp(F.regexp_extract("source_file", r"extract_ts=(\d{8}T\d{6}Z)", 1),
                                     "yyyyMMdd'T'HHmmss'Z'"))
          .withColumn("batch_id", F.lit(batch_id))
          .withColumn("load_type", F.lit(load_type))
          .withColumn("load_timestamp", F.current_timestamp()))

    # 3. Write in the column order of the table (columns added by earlier drift handling stay valid).
    target_cols = spark.table(BRONZE_TBL).columns
    df = df.select(*[F.col(c) if c in df.columns else F.lit(None).cast("string").alias(c)
                     for c in target_cols])
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"batch_id = '{batch_id}'")
       .saveAsTable(BRONZE_TBL))

    # 4. Reconcile with the landing manifest.
    in_bronze = spark.table(BRONZE_TBL).where(F.col("batch_id") == batch_id).count()
    run.rows_inserted = in_bronze
    run.rows_read = manifest["rows"] if manifest else in_bronze
    if in_bronze != run.rows_read:
        raise ValueError(f"Row count mismatch: landed {run.rows_read}, Bronze {in_bronze}")

    # 5. Schema drift handling.
    rescued = spark.table(BRONZE_TBL).where((F.col("batch_id") == batch_id) & F.col("_rescued_data").isNotNull())
    drift_rows = rescued.count()
    if drift_rows:
        keys = sorted({r[0] for r in rescued.select(F.explode(F.expr("json_object_keys(_rescued_data)")))
                       .distinct().collect()} - {"_file_path"})
        known = set(spark.table(BRONZE_TBL).columns)
        new_cols = [k for k in keys if k not in known and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k)]
        other = [k for k in keys if k not in new_cols]
        run.note(f"SCHEMA DRIFT: {drift_rows} rows with rescued data. New columns: {new_cols}. "
                 f"Conflicting or unusable keys: {other}")
        if new_cols and schema_drift_mode == "evolve":
            for c in new_cols:
                spark.sql(f"ALTER TABLE {BRONZE_TBL} ADD COLUMNS (`{c}` STRING)")
            sets = ", ".join(f"`{c}` = get_json_object(_rescued_data, '$.{c}')" for c in new_cols)
            spark.sql(f"UPDATE {BRONZE_TBL} SET {sets} WHERE batch_id = '{batch_id}' AND _rescued_data IS NOT NULL")
            run.note(f"Bronze schema evolved with columns {new_cols}")
        else:
            run.note("Drift left in _rescued_data (log_only mode or unusable key names)")

    if purge_landing and not landing_path:
        dbutils.fs.rm(batch_dir, True)
        run.note("landing files purged after successful load")

dbutils.notebook.exit(json.dumps({"batch_id": batch_id, "rows": in_bronze, "drift_rows": drift_rows}))
