import requests, pathlib
from datetime import datetime, timedelta, timezone

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9"
out = pathlib.Path(__file__).resolve().parent.parent / "sample_data"
out.mkdir(exist_ok=True)

def get(ext, params):
    r = requests.get(f"{BASE}.{ext}", params=params, timeout=120)
    r.raise_for_status()
    return r

full_where = "created_date between '2026-02-01T00:00:00' and '2026-02-02T00:00:00'"
r = get("csv", {"$where": full_where, "$order": "unique_key", "$limit": 5000})
(out / "full_load_sample.csv").write_bytes(r.content)

start = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")
inc_where = f"created_date >= '{start}' OR resolution_action_updated_date >= '{start}'"
r = get("json", {"$where": inc_where, "$order": "unique_key DESC", "$limit": 5000})
(out / "incremental_sample.json").write_bytes(r.content)

def count(where):
    return int(get("json", {"$select": "count(*)", "$where": where}).json()[0]["count"])

print("Baseline rows:", count("created_date between '2026-02-01T00:00:00' and '2026-10-04T00:00:00'"))
print("Incremental window rows:", count(inc_where))
print("Saved to:", out)