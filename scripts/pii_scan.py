import pandas as pd, re

df = pd.read_csv("sample_data/full_load_sample.csv", dtype=str)
phone = re.compile(r"(\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
email = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
unit  = re.compile(r"\b(apt|apartment|unit|fl|floor|#)\s*\w+", re.I)
house = re.compile(r"^\d+\s+\w+")

desc = df["resolution_description"].dropna()
addr = df["incident_address"].dropna()
print("rows scanned (desc):", len(desc), " (addr):", len(addr))
print("phone in resolution_description:", desc.str.contains(phone).sum())
print("email in resolution_description:", desc.str.contains(email).sum())
print("unit text in incident_address:", addr.str.contains(unit).sum())
print("exact street address in incident_address:", addr.str.contains(house).sum())
print(desc.sample(50, random_state=1).to_string())   # read these by hand for names