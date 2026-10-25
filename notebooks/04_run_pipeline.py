# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Run pipeline (orchestrator)
# MAGIC Runs the stages in order, passing the same parameters to each. Every stage notebook can also be run on
# MAGIC its own. Use `stages` to run only part of the pipeline.
# MAGIC
# MAGIC | Goal | Parameters |
# MAGIC |---|---|
# MAGIC | Standard daily incremental load | `load_type=INCREMENTAL`, everything else default |
# MAGIC | Backfill one month (API to Silver) | `load_type=FULL`, `start_date=2026-03-01`, `end_date=2026-04-01` |
# MAGIC | Re-run Bronze to Silver for one batch | `stages=silver`, `batch_id=FULL_20260301_20260401` |
# MAGIC | Re-run Raw to Bronze for one batch | `stages=bronze,silver`, `batch_id=...`, `load_type=...` |
# MAGIC | Quick test | `max_pages=1` |

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

stages        = [s.strip() for s in get_param("stages", "landing,bronze,silver,gold").split(",")]
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
gold_full     = get_param("gold_full_refresh", "false")
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

# Gold refreshes right after Silver. It has its own watermark, so if it fails it simply catches up next run.
if "gold" in stages:
    run_stage("./06_silver_to_gold", dict(full_refresh=gold_full,
                                          run_label=(f"GOLD_{batch_id}" if batch_id else "")))

# COMMAND ----------

display(spark.sql(f"""
    SELECT layer, batch_id, status, rows_read, rows_inserted, rows_updated, rows_quarantined,
           start_time, end_time, duration_seconds
    FROM {LOG_TBL} ORDER BY start_time DESC LIMIT 20"""))
