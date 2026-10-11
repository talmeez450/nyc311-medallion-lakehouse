import sys, json, types
sys.path.insert(0, "notebooks")
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F
import _common as c

spark = SparkSession.builder.master("local[1]").config("spark.sql.shuffle.partitions","2").config("spark.sql.session.timeZone","UTC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---- pure functions
print(c.resolve_window("FULL","2026-03-01","2026-04-01","7",None))
print(c.resolve_window("INCREMENTAL","","2026-10-05","7",datetime(2026,10,4)))   # watermark newer than floor -> floor wins
print(c.resolve_window("INCREMENTAL","","2026-10-05","7",datetime(2026,9,20)))   # old watermark -> recovers missed days
print(c.make_batch_id("FULL","2026-03-01T00:00:00","2026-04-01T00:00:00"))
print(c.build_where("INCREMENTAL","2026-09-27T00:00:00","2026-10-04T12:00:00","2026-02-01T00:00:00"))
try: c.resolve_window("FULL","","", "7", None)
except ValueError as e: print("ok error:", e)
try: c.safe_id("a'; drop")
except ValueError as e: print("ok error:", e)

# ---- cleansing logic on synthetic bronze rows
def row(**kw):
    base = {col: None for col in c.SOURCE_COLUMNS}
    base.update(kw)
    base.update(dict(_rescued_data=kw.pop("_rescued_data", None)))
    return base
ext = datetime(2026,10,4,12,0,0)
rows = [
 row(unique_key="1", created_date="2026-10-01T10:00:00.000", agency="nypd", borough="brooklyn", incident_zip="11201-1234", latitude="40.69412", longitude="-73.98765", status="Open", incident_address="123", street_name="MAIN ST", resolution_description="call 212-555-1212"),
 row(unique_key="1", created_date="2026-10-01T10:00:00.000", agency="nypd", borough="brooklyn", incident_zip="11201", latitude="40.69412", longitude="-73.98765", status="Closed", closed_date="2026-10-03T09:00:00.000", resolution_action_updated_date="2026-10-03T09:00:00.000"),
 row(unique_key="2", created_date="garbage"),
 row(unique_key="3", created_date="2026-10-02T10:00:00.000", latitude="0", longitude="0"),
 row(unique_key="4", created_date="2026-10-02T10:00:00.000", latitude="abc", longitude="-73.9"),
 row(unique_key="5", created_date="2026-10-02T10:00:00.000", resolution_action_updated_date="2026-12-14T11:50:00.000"),   # future date
 row(unique_key="6", created_date="2026-10-02T10:00:00.000", _rescued_data='{"incident_zip":{"a":1},"_file_path":"x"}'),    # conflict
 row(unique_key="7", created_date="2026-10-02T10:00:00.000", _rescued_data='{"brand_new":"x","_file_path":"x"}'),             # new col only
 row(unique_key=" ", created_date="2026-10-02T10:00:00.000"),                                                                  # blank key
 row(unique_key="8", created_date="2026-10-02T10:00:00.000"),                                                                  # no coords at all -> must be kept
]
# extract / load timestamps
data = []
for i, r in enumerate(rows):
    r["_rescued_data"] = r.get("_rescued_data")
    r["source_file"] = "f"; r["batch_id"] = "B1"; r["load_type"] = "INCREMENTAL"
    r["extract_timestamp"] = ext + timedelta(hours=1 if i == 1 else 0)   # second version of key 1 is newer
    r["load_timestamp"] = ext
    data.append(r)
from pyspark.sql.types import *
schema = StructType([StructField(x, StringType()) for x in c.SOURCE_COLUMNS] + [
    StructField("_rescued_data", StringType()), StructField("source_file", StringType()),
    StructField("extract_timestamp", TimestampType()), StructField("batch_id", StringType()),
    StructField("load_type", StringType()), StructField("load_timestamp", TimestampType())])
bronze = spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in data], schema)

flagged = c.cleanse(bronze)
q = c.to_quarantine(flagged)
print("--- quarantine"); q.select("unique_key","reason").show(truncate=False)
good = c.dedupe_latest(c.to_silver_columns(flagged.where("quarantine_reason IS NULL")))
print("--- silver columns match DDL:", good.columns == c.SILVER_COLUMN_NAMES)
good.select("unique_key","status","created_ts","closed_ts","resolution_action_updated_ts","is_future_dated","agency","borough","incident_zip","latitude","longitude").orderBy("unique_key").show(truncate=False)
assert not any(p in good.columns for p in c.PII_COLUMNS), "PII leaked"
# idempotency: same input twice -> identical row_hash set
h1 = sorted(r[0] for r in good.select("row_hash").collect())
good2 = c.dedupe_latest(c.to_silver_columns(c.cleanse(bronze).where("quarantine_reason IS NULL")))
h2 = sorted(r[0] for r in good2.select("row_hash").collect())
print("hash stable across runs:", h1 == h2)
print("dup keys after dedupe:", good.groupBy("unique_key").count().where("count>1").count())
