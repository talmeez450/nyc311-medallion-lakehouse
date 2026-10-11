# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Source to Landing (NYC Open Data API)
# MAGIC Calls the Socrata API from inside Databricks and saves each page, unchanged, as a JSON file in the
# MAGIC landing volume. Nothing is downloaded to a local machine.
# MAGIC
# MAGIC **Paging.** Keyset pagination (`unique_key > last key`, ordered by `unique_key`) instead of `$offset`,
# MAGIC so rows cannot be skipped or repeated when the source changes during a long download.
# MAGIC
# MAGIC **Idempotent.** The batch folder is replaced on every run, and a `_manifest.json` is written only after
# MAGIC every page succeeded. Bronze refuses a batch without a manifest.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

load_type     = get_param("load_type", "INCREMENTAL").upper()    # FULL | INCREMENTAL
start_date    = get_param("start_date", "")                      # FULL: required. INCREMENTAL: optional override
end_date      = get_param("end_date", "")                        # FULL: required. INCREMENTAL: default now
lookback_days = get_param("lookback_days", "7")                  # INCREMENTAL look-back floor
scope_start   = get_param("scope_start", DEFAULT_SCOPE_START)    # INCREMENTAL: ignore tickets created before this
batch_id      = get_param("batch_id", "")                        # default is derived from load_type and window
page_size     = min(int(get_param("page_size", "25000")), API_MAX_PAGE_SIZE)
max_pages     = int(get_param("max_pages", "0"))                 # 0 = all pages (use 1 for a quick test)
app_token     = get_param("app_token", "")                       # optional Socrata app token, never logged

# COMMAND ----------

def land_batch(where, batch_dir, page_size, max_pages, token, meta):
    extract_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    try:
        dbutils.fs.rm(batch_dir, True)           # replace any earlier attempt of this batch
    except Exception:
        pass
    out_dir = f"{batch_dir}/extract_ts={extract_ts}"
    os.makedirs(out_dir, exist_ok=True)

    last_key, pages, total = None, 0, 0
    while True:
        page_where = f"({where})" + (f" AND unique_key > '{last_key}'" if last_key else "")
        resp = api_get({"$where": page_where, "$order": "unique_key", "$limit": page_size}, token)
        rows = resp.json()
        if not rows:
            break
        pages += 1
        with open(f"{out_dir}/page_{pages:05d}.json", "wb") as f:
            f.write(resp.content)                # raw bytes, exactly as the API returned them
        total += len(rows)
        last_key = rows[-1]["unique_key"]
        print(f"page {pages}: {len(rows)} rows (total {total})")
        if len(rows) < page_size or (max_pages and pages >= max_pages):
            break

    manifest = dict(meta, extract_ts=extract_ts, pages=pages, rows=total,
                    truncated=bool(max_pages and pages >= max_pages),
                    completed_at=datetime.now(timezone.utc).isoformat())
    with open(f"{batch_dir}/_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    return manifest

# COMMAND ----------

watermark = read_watermark() if load_type == "INCREMENTAL" else None
window_start, window_end = resolve_window(load_type, start_date, end_date, lookback_days, watermark)
batch_id = safe_id(batch_id or make_batch_id(load_type, window_start, window_end))
where = build_where(load_type, window_start, window_end, scope_start)
batch_dir = f"{LANDING_ROOT}/{load_type.lower()}/{batch_id}"

params = dict(load_type=load_type, start_date=window_start, end_date=window_end, lookback_days=lookback_days,
              scope_start=scope_start, batch_id=batch_id, page_size=page_size, max_pages=max_pages,
              app_token=app_token)

with PipelineRun("Source-to-Landing", load_type, batch_id, batch_dir, params) as run:
    run.note(f"where: {where}")
    manifest = land_batch(where, batch_dir, page_size, max_pages, app_token,
                          meta=dict(batch_id=batch_id, load_type=load_type, window_start=window_start,
                                    window_end=window_end, where=where, api=API_URL, page_size=page_size))
    run.rows_read = run.rows_inserted = manifest["rows"]
    run.note(f"pages={manifest['pages']} truncated={manifest['truncated']}")
    # The watermark is NOT moved here. 04_run_pipeline moves it only after Bronze and Silver succeeded.

dbutils.notebook.exit(json.dumps({"batch_id": batch_id, "load_type": load_type,
                                  "rows": manifest["rows"], "pages": manifest["pages"],
                                  "window_end": window_end, "truncated": manifest["truncated"]}))
