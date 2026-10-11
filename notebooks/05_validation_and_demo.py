# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Validation and demonstration
# MAGIC Evidence for the Phase 2 requirements: no duplicates, idempotent reruns, schema drift handling,
# MAGIC quarantine, and the audit log. The drift demo uses a small synthetic batch (id `DEMO_DRIFT`) that is
# MAGIC created inside Databricks and removed at the end, so it never mixes with real data.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Integrity checks on real data

# COMMAND ----------

print("Silver rows:", spark.table(SILVER_TBL).count())
print("Duplicate unique_key in Silver (must be 0):",
      spark.sql(f"SELECT COUNT(*) FROM (SELECT unique_key FROM {SILVER_TBL} GROUP BY unique_key HAVING COUNT(*) > 1)").collect()[0][0])
print("Rows without load_timestamp - Bronze / Silver (must be 0):",
      spark.table(BRONZE_TBL).where("load_timestamp IS NULL").count(),
      spark.table(SILVER_TBL).where("load_timestamp IS NULL").count())
display(spark.sql(f"SELECT reason, COUNT(*) AS rows FROM {QUARANTINE_TBL} GROUP BY reason ORDER BY rows DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Idempotency: run Bronze to Silver again on the same batch
# MAGIC Pick a batch that is already in Silver. The second run must report 0 inserted and 0 updated.

# COMMAND ----------

demo_batch = get_param("demo_batch_id", "")      # for example FULL_20260301_20260401
if demo_batch:
    before = spark.table(SILVER_TBL).count()
    first = json.loads(dbutils.notebook.run("./03_bronze_to_silver", 3600, {"batch_id": demo_batch}))
    second = json.loads(dbutils.notebook.run("./03_bronze_to_silver", 3600, {"batch_id": demo_batch}))
    after = spark.table(SILVER_TBL).count()
    print("first run :", first)
    print("second run:", second, "(inserted and updated must both be 0)")
    print("Silver rows before / after:", before, after)
else:
    print("Set the widget demo_batch_id to run this check.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema drift and quarantine demo (synthetic batch)

# COMMAND ----------

demo_id, demo_ts = "DEMO_DRIFT", "20260101T000000Z"
demo_dir = f"{LANDING_ROOT}/incremental/{demo_id}"
dbutils.fs.rm(demo_dir, True)
os.makedirs(f"{demo_dir}/extract_ts={demo_ts}", exist_ok=True)
records = [
    # good record with a NEW column the schema does not know
    {"unique_key": "DEMO0001", "created_date": "2026-01-01T10:00:00.000", "agency": "NYPD", "status": "Open",
     "borough": "BROOKLYN", "incident_zip": "11201", "latitude": "40.69", "longitude": "-73.99",
     "new_source_column": "hello"},
    # bad date -> quarantine BAD_CREATED_DATE
    {"unique_key": "DEMO0002", "created_date": "not-a-date", "agency": "DSNY", "status": "Open"},
    # coordinates outside NYC -> quarantine BAD_COORDINATES
    {"unique_key": "DEMO0003", "created_date": "2026-01-01T11:00:00.000", "agency": "DOT", "status": "Open",
     "latitude": "0", "longitude": "0"},
    # latitude arrives as text -> quarantine BAD_NUMERIC
    {"unique_key": "DEMO0004", "created_date": "2026-01-01T12:00:00.000", "agency": "HPD", "status": "Open",
     "latitude": "forty", "longitude": "-73.9"},
]
json.dump(records, open(f"{demo_dir}/extract_ts={demo_ts}/page_00001.json", "w"))
json.dump({"batch_id": demo_id, "rows": len(records), "pages": 1, "truncated": False}, open(f"{demo_dir}/_manifest.json", "w"))

print(dbutils.notebook.run("./02_landing_to_bronze", 3600, {"load_type": "INCREMENTAL", "batch_id": demo_id, "schema_drift_mode": "evolve"}))
print(dbutils.notebook.run("./03_bronze_to_silver", 3600, {"batch_id": demo_id}))

# COMMAND ----------

display(spark.sql(f"SELECT unique_key, reason, record_json FROM {QUARANTINE_TBL} WHERE batch_id = '{demo_id}'"))
display(spark.sql(f"SELECT unique_key, status, latitude, longitude FROM {SILVER_TBL} WHERE batch_id = '{demo_id}'"))
display(spark.sql(f"SELECT unique_key, new_source_column FROM {BRONZE_TBL} WHERE batch_id = '{demo_id}' AND new_source_column IS NOT NULL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Clean up the demo data (the evolved Bronze column stays, which is what schema evolution means)

# COMMAND ----------

spark.sql(f"DELETE FROM {SILVER_TBL} WHERE batch_id = '{demo_id}'")
spark.sql(f"DELETE FROM {BRONZE_TBL} WHERE batch_id = '{demo_id}'")
spark.sql(f"DELETE FROM {QUARANTINE_TBL} WHERE batch_id = '{demo_id}'")
dbutils.fs.rm(demo_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Audit log

# COMMAND ----------

display(spark.sql(f"""
    SELECT layer, load_type, batch_id, status, rows_read, rows_inserted, rows_updated, rows_quarantined,
           start_time, end_time, duration_seconds, error_message
    FROM {LOG_TBL} ORDER BY start_time DESC LIMIT 30"""))
