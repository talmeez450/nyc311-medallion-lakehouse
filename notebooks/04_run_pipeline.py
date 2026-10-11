# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Run pipeline (orchestrator)

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

stages        = [s.strip() for s in get_param("stages", "landing,bronze,silver").split(",")]
load_type     = get_param("load_type", "INCREMENTAL").upper()
start_date    = get_param("start_date", "")
end_date      = get_param("end_date", "")
lookback_days = get_param("lookback_days", "7")
scope_start   = get_param("scope_start", DEFAULT_SCOPE_START)
batch_id      = get_param("batch_id", "")
page_size     = get_param("page_size", "25000")
max_pages     = get_param("max_pages", "0")
app_token     = get_param("app_token", "")
drift_mode    = get_param("schema_drift_mode", "evolve")
purge         = get_param("purge_landing", "false")
timeout       = int(get_param("stage_timeout_seconds", "7200"))

# COMMAND ----------

def run_stage(path, args):
    print(f"--> {path} {redact(args)}")
    out = dbutils.notebook.run(path, timeout, args)
    print("    result:", out)
    return json.loads(out) if out else {}

# COMMAND ----------

if "landing" in stages:
    landed = run_stage("./01_api_to_landing", dict(
        load_type=load_type, start_date=start_date, end_date=end_date, lookback_days=lookback_days,
        scope_start=scope_start, batch_id=batch_id, page_size=page_size, max_pages=max_pages,
        app_token=app_token))
    batch_id = landed["batch_id"]          # use the id the landing step derived

if "bronze" in stages:
    run_stage("./02_landing_to_bronze", dict(load_type=load_type, batch_id=batch_id,
                                             schema_drift_mode=drift_mode, purge_landing=purge))

if "silver" in stages:
    run_stage("./03_bronze_to_silver", dict(batch_id=batch_id or "ALL"))

if {"landing", "bronze", "silver"} <= set(stages) and not landed.get("truncated"):
    advance_watermark(parse_ts(landed["window_end"]))
    print("watermark advanced to", landed["window_end"])

# COMMAND ----------

display(spark.sql(f"""
    SELECT layer, batch_id, status, rows_read, rows_inserted, rows_updated, rows_quarantined,
           start_time, end_time, duration_seconds
    FROM {LOG_TBL} ORDER BY start_time DESC LIMIT 10"""))