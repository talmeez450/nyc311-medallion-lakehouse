# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Gold checks, optimisation, BI views and evidence
# MAGIC Run after `06_silver_to_gold`. Sections:
# MAGIC 1. Integrity checks (row counts, orphan keys, duplicates)
# MAGIC 2. Check complaint types and the Rodent rule
# MAGIC 3. OPTIMIZE (liquid clustering) with before and after file counts
# MAGIC 4. Query plans (shuffle, broadcast join, predicate pushdown) for the report
# MAGIC 5. Views for the BI audit pages
# MAGIC 6. FinOps numbers (run times from the audit log)

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Integrity checks. Every "must be 0" line must show 0.

# COMMAND ----------

print("Silver rows            :", spark.table(SILVER_TBL).count())
print("Fact rows              :", spark.table(GOLD_FACT_TBL).count(), "(equals Silver after a full refresh)")
print("Duplicate unique_key in fact (must be 0):",
      spark.sql(f"SELECT COUNT(*) FROM (SELECT unique_key FROM {GOLD_FACT_TBL} GROUP BY unique_key HAVING COUNT(*) > 1)").collect()[0][0])
orphans = {
    "location": f"SELECT COUNT(*) FROM {GOLD_FACT_TBL} f LEFT ANTI JOIN {GOLD_DIM_LOCATION_TBL} d ON f.location_key = d.location_key",
    "agency": f"SELECT COUNT(*) FROM {GOLD_FACT_TBL} f LEFT ANTI JOIN {GOLD_DIM_AGENCY_TBL} d ON f.agency_key = d.agency_key",
    "complaint_type": f"SELECT COUNT(*) FROM {GOLD_FACT_TBL} f LEFT ANTI JOIN {GOLD_DIM_TYPE_TBL} d ON f.complaint_type_key = d.complaint_type_key",
    "created date": f"SELECT COUNT(*) FROM {GOLD_FACT_TBL} f LEFT ANTI JOIN {GOLD_DIM_TIME_TBL} d ON f.created_date_key = d.date_key",
}
for name, sql in orphans.items():
    # LEFT ANTI JOIN returns fact rows with no matching dimension row
    n = spark.sql(sql.replace("SELECT COUNT(*) FROM", "SELECT COUNT(*) AS n FROM")).collect()[0][0]
    print(f"Orphan {name} keys (must be 0):", n)
print("Negative resolution hours (must be 0):",
      spark.sql(f"SELECT COUNT(*) FROM {GOLD_FACT_TBL} WHERE resolution_hours < 0").collect()[0][0])
print("Aggregate vs fact request count:",
      spark.sql(f"SELECT SUM(requests_created) FROM {GOLD_AGG_TBL}").collect()[0][0],
      spark.table(GOLD_FACT_TBL).count())
display(spark.sql(f"SELECT 'dim_time' t, COUNT(*) n FROM {GOLD_DIM_TIME_TBL} UNION ALL SELECT 'dim_location', COUNT(*) FROM {GOLD_DIM_LOCATION_TBL} "
                  f"UNION ALL SELECT 'dim_agency', COUNT(*) FROM {GOLD_DIM_AGENCY_TBL} UNION ALL SELECT 'dim_complaint_type', COUNT(*) FROM {GOLD_DIM_TYPE_TBL} "
                  f"UNION ALL SELECT 'fact_311_requests', COUNT(*) FROM {GOLD_FACT_TBL} UNION ALL SELECT 'agg_daily_requests', COUNT(*) FROM {GOLD_AGG_TBL}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Complaint types: check that the grouping rules fit your data
# MAGIC Look at the biggest complaint types and the group each one received. If Rodent complaints are not in the
# MAGIC `Rodent` group, edit `COMPLAINT_GROUP_RULES` in `_common`, then run `06_silver_to_gold` with `full_refresh=true`.

# COMMAND ----------

display(spark.sql(f"""
    SELECT d.complaint_group, d.complaint_type, COUNT(*) AS requests
    FROM {GOLD_FACT_TBL} f JOIN {GOLD_DIM_TYPE_TBL} d ON f.complaint_type_key = d.complaint_type_key
    GROUP BY d.complaint_group, d.complaint_type ORDER BY requests DESC LIMIT 40"""))
display(spark.sql(f"""
    SELECT d.complaint_group, COUNT(*) AS requests
    FROM {GOLD_FACT_TBL} f JOIN {GOLD_DIM_TYPE_TBL} d ON f.complaint_type_key = d.complaint_type_key
    GROUP BY d.complaint_group ORDER BY requests DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. OPTIMIZE. The fact table is liquid-clustered by (created_date_key, agency_key).
# MAGIC The first query shows the file layout before, the second after. Keep both outputs for the report.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {GOLD_FACT_TBL}").select("numFiles", "sizeInBytes", "clusteringColumns"))

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {GOLD_FACT_TBL}"))
display(spark.sql(f"OPTIMIZE {GOLD_AGG_TBL}"))
display(spark.sql(f"DESCRIBE DETAIL {GOLD_FACT_TBL}").select("numFiles", "sizeInBytes", "clusteringColumns"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Query plans for the report
# MAGIC Serverless compute has no Spark UI. The query profile (click "See performance" under a cell, then "See query
# MAGIC profile") shows the same DAG, and `explain` prints the physical plan. In the plan look for:
# MAGIC * `BroadcastHashJoin`: the small dimension is copied to every worker, so the big fact table is not shuffled for the join.
# MAGIC * `Exchange hashpartitioning`: a **shuffle**. Rows with the same group key are moved to the same worker for `GROUP BY`.
# MAGIC * `HashAggregate` appearing twice: partial aggregation before the shuffle, final aggregation after it.
# MAGIC * `PushedFilters` or `DataFilters` in the scan: **predicate pushdown**, so files that cannot match are skipped.

# COMMAND ----------

agg_plan_df = build_agg(spark.table(GOLD_FACT_TBL), spark.table(GOLD_DIM_LOCATION_TBL))
agg_plan_df.explain("formatted")

# COMMAND ----------

# A typical dashboard query with a date filter (predicate pushdown and file skipping on the clustered table)
filtered = spark.sql(f"""
    SELECT agency_key, COUNT(*) AS requests, ROUND(AVG(resolution_hours), 1) AS avg_hours
    FROM {GOLD_FACT_TBL} WHERE created_date_key >= 20260901 AND is_resolved
    GROUP BY agency_key""")
filtered.explain("formatted")
display(filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Views for the BI audit pages (Power BI reads these)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.vw_pipeline_runs AS
SELECT run_id, layer,
       CASE layer WHEN 'Source-to-Landing' THEN 1 WHEN 'Raw-to-Bronze' THEN 2
                  WHEN 'Bronze-to-Silver' THEN 3 WHEN 'Silver-to-Gold' THEN 4 ELSE 5 END AS layer_order,
       load_type, batch_id, source_ref, status, rows_read, rows_inserted, rows_updated, rows_quarantined,
       start_time, end_time, duration_seconds, error_message, CAST(start_time AS DATE) AS run_date
FROM {LOG_TBL}""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.vw_quarantine_summary AS
SELECT batch_id, reason, COUNT(*) AS quarantined_rows FROM {QUARANTINE_TBL} GROUP BY batch_id, reason""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.vw_layer_row_counts AS
SELECT 1 AS layer_order, 'Bronze' AS layer, COUNT(*) AS row_count FROM {BRONZE_TBL}
UNION ALL SELECT 2, 'Silver', COUNT(*) FROM {SILVER_TBL}
UNION ALL SELECT 3, 'Gold fact', COUNT(*) FROM {GOLD_FACT_TBL}
UNION ALL SELECT 4, 'Quarantine', COUNT(*) FROM {QUARANTINE_TBL}""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.vw_batch_reconciliation AS
SELECT batch_id,
       MAX(CASE WHEN layer = 'Source-to-Landing' AND status = 'Success' THEN rows_read END) AS rows_landed,
       MAX(CASE WHEN layer = 'Raw-to-Bronze' AND status = 'Success' THEN rows_inserted END) AS rows_in_bronze,
       SUM(CASE WHEN layer = 'Bronze-to-Silver' AND status = 'Success' THEN rows_inserted END) AS silver_inserted,
       SUM(CASE WHEN layer = 'Bronze-to-Silver' AND status = 'Success' THEN rows_updated END) AS silver_updated,
       SUM(CASE WHEN layer = 'Bronze-to-Silver' AND status = 'Success' THEN rows_quarantined END) AS silver_quarantined,
       SUM(CASE WHEN status = 'Failure' THEN 1 ELSE 0 END) AS failures
FROM {LOG_TBL} WHERE batch_id NOT LIKE 'GOLD_%' GROUP BY batch_id""")

display(spark.sql(f"SHOW VIEWS IN {FQ}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. FinOps numbers from the audit log
# MAGIC Run time per layer for each batch. A typical daily incremental run is one `INCREMENTAL_...` batch plus the Gold
# MAGIC run that followed it (`GOLD_INCREMENTAL_...`). Copy these numbers into the report.

# COMMAND ----------

display(spark.sql(f"""
    SELECT REPLACE(batch_id, 'GOLD_', '') AS batch, layer, ROUND(SUM(duration_seconds), 1) AS seconds,
           SUM(rows_read) AS rows_read
    FROM {LOG_TBL}
    WHERE status = 'Success' AND REPLACE(batch_id, 'GOLD_', '') LIKE 'INCREMENTAL_%'
    GROUP BY REPLACE(batch_id, 'GOLD_', ''), layer ORDER BY batch, layer"""))

display(spark.sql(f"""
    SELECT REPLACE(batch_id, 'GOLD_', '') AS batch, ROUND(SUM(duration_seconds) / 60, 2) AS total_minutes
    FROM {LOG_TBL}
    WHERE status = 'Success' AND REPLACE(batch_id, 'GOLD_', '') LIKE 'INCREMENTAL_%'
    GROUP BY REPLACE(batch_id, 'GOLD_', '') ORDER BY batch"""))
