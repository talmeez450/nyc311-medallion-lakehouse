import pandas as pd, re, requests
pd.set_option("display.max_colwidth", None)

df = pd.read_csv("sample_data/full_load_sample.csv", dtype=str)
desc = df["resolution_description"].dropna()

phone = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
email = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")

print("=== phone matches, with surrounding text (first 15) ===")
for t in desc[desc.str.contains(phone)].head(15):
    m = phone.search(t)
    print(repr(t[max(0, m.start() - 40): m.end() + 20]))

print("\n=== email matches ===")
for t in desc[desc.str.contains(email)]:
    print(email.search(t).group())

print("\n=== most common text templates ===")
top = desc.str[:60].value_counts().head(8)
print(top)

print("\n=== 15 descriptions outside the top templates, full text ===")
rest = desc[~desc.str[:60].isin(top.index)]
print(len(rest), "rows outside the top templates")
for t in rest.sample(min(15, len(rest)), random_state=2):
    print("-", t)

print("\n=== newest data in the source ===")
B = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
for col in ["created_date", "resolution_action_updated_date"]:
    print(col, requests.get(B, params={"$select": f"max({col})"}, timeout=120).json())