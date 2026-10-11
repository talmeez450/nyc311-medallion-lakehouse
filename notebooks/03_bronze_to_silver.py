# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Bronze to Silver

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

batch_param = get_param("batch_id", "ALL")        # one id, a comma separated list, or ALL
batch_ids = [] if batch_param.upper() == "ALL" else [safe_id(b.strip()) for b in batch_param.split(",")]
params = dict(batch_id=batch_param)

# COMMAND ----------

with PipelineRun("Bronze-to-Silver", "MIXED", batch_param, BRONZE_TBL, params) as run:
    src = spark.table(BRONZE_TBL)
    if batch_ids:
        src = src.where(F.col("batch_id").isin(batch_ids))
    run.rows_read = src.count()
    if run.rows_read == 0:
        raise ValueError(f"No Bronze rows found for batch_id={batch_param}")

    flagged = cleanse(src)

    # 1. Quarantine (replace the quarantine rows of these batches, so reruns do not duplicate them)
    quarantine = to_quarantine(flagged)
    if batch_ids:
        in_list = ", ".join(f"'{b}'" for b in batch_ids)
        (quarantine.write.format("delta").mode("overwrite")
                   .option("replaceWhere", f"batch_id IN ({in_list})").saveAsTable(QUARANTINE_TBL))
        run.rows_quarantined = spark.table(QUARANTINE_TBL).where(F.col("batch_id").isin(batch_ids)).count()
    else:
        quarantine.write.format("delta").mode("overwrite").saveAsTable(QUARANTINE_TBL)
        run.rows_quarantined = spark.table(QUARANTINE_TBL).count()

    # 2. Good rows: project to the Silver columns, keep the newest version per key
    good = dedupe_latest(to_silver_columns(flagged.where(F.col("quarantine_reason").isNull())))
    good.createOrReplaceTempView("staged_updates")

    # 3. MERGE INTO (UPSERT)
    result = spark.sql(f"""
        MERGE INTO {SILVER_TBL} AS t
        USING staged_updates AS s
        ON t.unique_key = s.unique_key
        WHEN MATCHED AND s.extract_timestamp >= t.extract_timestamp AND s.row_hash <> t.row_hash THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
    """).collect()[0].asDict()
    run.rows_inserted = int(result.get("num_inserted_rows") or 0)
    run.rows_updated = int(result.get("num_updated_rows") or 0)
    run.note(f"merge result: {result}")

dbutils.notebook.exit(json.dumps({"rows_read": run.rows_read, "inserted": run.rows_inserted,
                                  "updated": run.rows_updated, "quarantined": run.rows_quarantined}))