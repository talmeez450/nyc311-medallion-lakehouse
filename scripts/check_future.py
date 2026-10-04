import requests
B = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
def c(w):
    return requests.get(B, params={"$select": "count(*)", "$where": w}, timeout=120).json()[0]["count"]

now = "2026-10-04T00:00:00"
print("future-dated, all rows       :", c(f"resolution_action_updated_date > '{now}'"))
print("future-dated, since Feb 1    :", c(f"resolution_action_updated_date > '{now}' AND created_date >= '2026-02-01T00:00:00'"))
print("7-day window, old filter     :", c("created_date >= '2026-09-27T00:00:00' OR resolution_action_updated_date >= '2026-09-27T00:00:00'"))
print("7-day window, with upper bound:", c(f"created_date >= '2026-09-27T00:00:00' OR (resolution_action_updated_date >= '2026-09-27T00:00:00' AND resolution_action_updated_date <= '{now}')"))