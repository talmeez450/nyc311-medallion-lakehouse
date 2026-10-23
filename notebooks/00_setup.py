# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Can this workspace reach the API?

# COMMAND ----------

try:
    r = api_get({"$select": "count(*)", "$where": "created_date >= '2026-10-01T00:00:00'"})
    print("API reachable. Rows created since 2026-10-01:", r.json()[0]["count"])
except Exception as e:
    raise RuntimeError("Cannot reach data.cityofnewyork.us from this workspace. "
                       "See the instructions above this cell. Original error: " + str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema and landing volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables

# COMMAND ----------

def ddl(table, columns, comment):
    cols = ",\n  ".join(f"`{n}` {t}" for n, t, *_ in columns)
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n) USING DELTA COMMENT '{comment}'"

bronze_cols = [(c, "STRING") for c in SOURCE_COLUMNS] + [(n, t) for n, t, _ in BRONZE_META_COLUMNS]
spark.sql(ddl(BRONZE_TBL, bronze_cols, "Raw NYC 311 records, all source fields as STRING. Append-only per batch."))

spark.sql(ddl(SILVER_TBL, [(n, t) for n, t, _ in SILVER_COLUMNS],
              "Cleansed NYC 311 requests, one current row per unique_key. PII removed."))

spark.sql(ddl(QUARANTINE_TBL, [("unique_key", "STRING"), ("reason", "STRING"), ("batch_id", "STRING"),
                               ("extract_timestamp", "TIMESTAMP"), ("record_json", "STRING"),
                               ("load_timestamp", "TIMESTAMP")],
              "Records rejected by Bronze to Silver rules, with the reason."))

spark.sql(ddl(LOG_TBL, [(f.name, f.dataType.simpleString().upper()) for f in LOG_SCHEMA.fields],
              "One row per pipeline step execution."))

spark.sql(ddl(CONTROL_TBL, [("name", "STRING"), ("value_ts", "TIMESTAMP"), ("load_timestamp", "TIMESTAMP")],
              "Pipeline state, such as the incremental watermark."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informational primary key on Silver

# COMMAND ----------

try:
    spark.sql(f"ALTER TABLE {SILVER_TBL} ALTER COLUMN unique_key SET NOT NULL")
    spark.sql(f"ALTER TABLE {SILVER_TBL} ADD CONSTRAINT silver_requests_pk PRIMARY KEY (unique_key)")
    print("Primary key added on silver_requests.unique_key")
except Exception as e:
    print("Primary key constraint skipped (already present or not supported here):", str(e)[:200])

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {FQ}"))
