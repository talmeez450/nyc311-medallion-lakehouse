import requests, pandas as pd

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
start = "2026-09-27T00:00:00"

def q(extra):
    p = {"$select": "status,count(*)", "$group": "status",
         "$where": f"created_date >= '{start}' AND {extra}"}
    s = pd.DataFrame(requests.get(BASE, params=p, timeout=120).json())
    return s.set_index("status")["count"].astype(int)

res = pd.DataFrame({
    "update_date_filled": q("resolution_action_updated_date IS NOT NULL"),
    "update_date_null": q("resolution_action_updated_date IS NULL"),
}).fillna(0).astype(int)
print(res)