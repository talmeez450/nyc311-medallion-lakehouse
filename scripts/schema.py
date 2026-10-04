import requests, pandas as pd

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9"

# 1. which status values exist (last 7 days)
r = requests.get(f"{BASE}.json", params={
    "$select": "status,count(*)", "$group": "status",
    "$where": "created_date >= '2026-09-27T00:00:00'"})
print(pd.DataFrame(r.json()), "\n")

# 2. how often is resolution_action_updated_date filled, by status?
df = pd.read_json("sample_data/incremental_sample.json", dtype=str)
print(df.columns.tolist(), "\n")
df["has_update_date"] = df.get("resolution_action_updated_date").notna()
df["has_closed_date"] = df.get("closed_date").notna()
print(df.groupby("status")[["has_update_date", "has_closed_date"]].agg(["sum", "count"]))