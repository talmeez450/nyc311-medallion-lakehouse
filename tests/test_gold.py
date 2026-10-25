import sys
sys.path.insert(0, "notebooks")
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import _common as c

spark = SparkSession.builder.master("local[1]").config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([StructField(n, t) for n, t in [
    ("unique_key", StringType()), ("created_ts", TimestampType()), ("closed_ts", TimestampType()),
    ("agency", StringType()), ("agency_name", StringType()), ("complaint_type", StringType()),
    ("status", StringType()), ("borough", StringType()), ("incident_zip", StringType()),
    ("latitude", DoubleType()), ("longitude", DoubleType()), ("row_hash", StringType()),
    ("extract_timestamp", TimestampType()), ("load_timestamp", TimestampType())]])
t = lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
rows = [
    ("1", t("2026-10-01 10:00:00"), t("2026-10-01 22:00:00"), "NYPD", "Police Department", "Noise - Residential", "Closed", "BROOKLYN", "11201", 40.69, -73.99, "h1", t("2026-10-11 00:00:00"), t("2026-10-11 01:00:00")),
    ("2", t("2026-10-01 11:00:00"), None, "DSNY", "Sanitation", "Rodent", "Open", "QUEENS", "11375", 40.72, -73.84, "h2", t("2026-10-11 00:00:00"), t("2026-10-11 01:00:00")),
    ("3", t("2026-10-02 09:00:00"), None, None, None, None, "In Progress", None, None, None, None, "h3", t("2026-10-11 00:00:00"), t("2026-10-11 01:00:00")),   # all unknown
    ("4", t("2026-10-02 09:00:00"), t("2026-10-02 08:00:00"), "NYPD", "Police Department", "Illegal Parking", "Closed", "BROOKLYN", "11201", 40.70, -73.98, "h4", t("2026-10-11 00:00:00"), t("2026-10-11 01:00:00")),  # closed before created
    ("5", t("2026-10-03 09:00:00"), t("2026-10-03 10:00:00"), "NYPD", "Police Department", "Noise - Street/Sidewalk", "Assigned", "BROOKLYN", "11201", 40.68, -73.97, "h5", t("2026-10-11 00:00:00"), t("2026-10-11 01:00:00")),  # closed_ts but not Closed
]
silver = spark.createDataFrame(rows, schema)
prep = c.gold_prepare(silver)
fact = c.to_fact(prep)
print("fact columns match DDL:", fact.columns == [x[0] for x in c.FACT_COLUMNS])
f = {r["unique_key"]: r for r in fact.collect()}
assert f["1"]["resolution_hours"] == 12.0 and f["1"]["is_resolved"] and f["1"]["closed_date_key"] == 20261001
assert f["2"]["is_resolved"] is False and f["2"]["closed_date_key"] is None and f["2"]["resolution_hours"] is None
assert f["4"]["resolution_hours"] is None, "negative duration must not be reported"
assert f["5"]["is_resolved"] is False and f["5"]["closed_date_key"] is None, "closed_ts without status Closed is not resolved"
assert f["1"]["created_hour"] == 10 and f["1"]["created_date_key"] == 20261001 and f["1"]["request_count"] == 1
print("fact rules OK")

# deterministic keys + unknown member
k1 = [r["agency_key"] for r in c.gold_prepare(silver).where("unique_key='1'").collect()]
k2 = [r["agency_key"] for r in c.gold_prepare(silver).where("unique_key='1'").collect()]
assert k1 == k2 and k1[0] == f["4"]["agency_key"], "same agency -> same key"
da = c.to_dim_agency(prep); assert da.columns == [x[0] for x in c.DIM_AGENCY_COLUMNS]
assert da.where("agency_code='UNKNOWN'").count() == 1 and da.count() == 3
dl = c.to_dim_location(prep); assert dl.columns == [x[0] for x in c.DIM_LOCATION_COLUMNS]
assert dl.where("borough='UNKNOWN' AND zip_code='UNKNOWN'").count() == 1 and dl.count() == 3
dt = c.to_dim_type(prep); assert dt.columns == [x[0] for x in c.DIM_TYPE_COLUMNS]
groups = {r["complaint_type"]: r["complaint_group"] for r in dt.collect()}
print(groups)
assert groups["Noise - Residential"] == "Noise" and groups["Rodent"] == "Rodent" and groups["Illegal Parking"] == "Illegal Parking" and groups["UNKNOWN"] == "Other"
# every fact key exists in its dimension (no orphans)
for fk, dim, dk in [("agency_key", da, "agency_key"), ("location_key", dl, "location_key"), ("complaint_type_key", dt, "complaint_type_key")]:
    assert fact.join(dim, fact[fk] == dim[dk], "left_anti").count() == 0, fk
print("dimensions OK, no orphan keys")

dtm = c.build_dim_time(spark, "2026-01-01", "2026-12-31")
assert dtm.columns == [x[0] for x in c.DIM_TIME_COLUMNS] and dtm.count() == 365
d = {r["date_key"]: r for r in dtm.collect()}
assert d[20261003]["is_weekend"] is True and d[20261001]["is_weekend"] is False and d[20261001]["year_month"] == "2026-10"
assert fact.join(dtm, fact.created_date_key == dtm.date_key, "left_anti").count() == 0
print("dim_time OK")

agg = c.build_agg(fact, dl)
assert agg.columns == [x[0] for x in c.AGG_COLUMNS]
assert agg.agg(F.sum("requests_created")).collect()[0][0] == 5
assert agg.agg(F.sum("requests_closed")).collect()[0][0] == 2   # tickets 1 and 4 are Closed
a = agg.where("borough='BROOKLYN' AND created_date_key=20261001").collect()[0]
assert a["requests_created"] == 1 and a["requests_closed"] == 1 and a["total_resolution_hours"] == 12.0 and a["resolved_with_duration"] == 1
print("aggregate OK")
import io
from contextlib import redirect_stdout
buf = io.StringIO()
with redirect_stdout(buf):
    agg.explain("formatted")
plan = buf.getvalue()
print("plan has broadcast join:", "BroadcastHashJoin" in plan, "| shuffle (Exchange):", "Exchange" in plan)
print("ALL GOLD TESTS PASSED")
