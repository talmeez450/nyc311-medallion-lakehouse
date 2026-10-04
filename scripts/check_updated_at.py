import requests

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

def c(where):
    r = requests.get(BASE, params={"$select": "count(*)", "$where": where}, timeout=120)
    return r.json()[0]["count"] if r.ok else "ERROR: " + r.text[:200]

d = "2026-10-03T00:00:00"
print("system :updated_at >= d      :", c(f":updated_at >= '{d}'"))
print("created or res_update >= d   :", c(f"created_date >= '{d}' OR resolution_action_updated_date >= '{d}'"))
print("total rows in dataset        :", c("unique_key IS NOT NULL"))