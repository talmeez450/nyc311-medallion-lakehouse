# Databricks notebook source
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                               StructField, StructType, TimestampType)

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "nyc311"
VOLUME = "landing"
FQ = f"{CATALOG}.{SCHEMA}"

BRONZE_TBL = f"{FQ}.bronze_requests"
SILVER_TBL = f"{FQ}.silver_requests"
QUARANTINE_TBL = f"{FQ}.silver_quarantine"
LOG_TBL = f"{FQ}.pipeline_execution_logs"
CONTROL_TBL = f"{FQ}.pipeline_control"
LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
API_MAX_PAGE_SIZE = 50000          # SODA limit per request
DEFAULT_SCOPE_START = "2026-02-01T00:00:00"   # start of the Phase 1 baseline

# COMMAND ----------

SOURCE_COLUMNS = [
    "unique_key", "created_date", "agency", "agency_name", "complaint_type", "descriptor",
    "descriptor_2", "location_type", "incident_zip", "incident_address", "street_name",
    "cross_street_1", "cross_street_2", "intersection_street_1", "intersection_street_2",
    "address_type", "city", "landmark", "status", "resolution_description",
    "resolution_action_updated_date", "community_board", "council_district", "police_precinct",
    "bbl", "borough", "x_coordinate_state_plane", "y_coordinate_state_plane",
    "open_data_channel_type", "park_facility_name", "park_borough", "latitude", "longitude",
    "location", "closed_date", "facility_type", "taxi_pick_up_location", "vehicle_type",
    "taxi_company_borough", "bridge_highway_name", "bridge_highway_direction", "road_ramp",
    "bridge_highway_segment", "due_date",
]

# StructType / StructField schema used when reading the raw JSON files
BRONZE_READ_SCHEMA = StructType([StructField(c, StringType(), True) for c in SOURCE_COLUMNS])

# Columns Bronze adds to every record
BRONZE_META_COLUMNS = [
    ("_rescued_data", "STRING", "JSON of fields that did not fit the read schema (schema drift evidence)"),
    ("source_file", "STRING", "Landing file the record was read from"),
    ("extract_timestamp", "TIMESTAMP", "When the API call was made (parsed from the landing path)"),
    ("batch_id", "STRING", "Batch the record belongs to"),
    ("load_type", "STRING", "FULL or INCREMENTAL"),
    ("load_timestamp", "TIMESTAMP", "When this record was written to Bronze"),
]

# Fields that can identify a person, a household or a building. They never leave Bronze.
PII_COLUMNS = [
    "incident_address", "street_name", "cross_street_1", "cross_street_2",
    "intersection_street_1", "intersection_street_2", "landmark", "location",
    "resolution_description", "bbl", "x_coordinate_state_plane", "y_coordinate_state_plane",
    "taxi_pick_up_location", "address_type",
]

# Silver data model: (name, type, description)
SILVER_COLUMNS = [
    ("unique_key", "STRING", "Primary key. Source identifier of the service request"),
    ("created_ts", "TIMESTAMP", "created_date cast to timestamp"),
    ("closed_ts", "TIMESTAMP", "closed_date cast to timestamp (NULL while open)"),
    ("due_ts", "TIMESTAMP", "due_date cast to timestamp"),
    ("resolution_action_updated_ts", "TIMESTAMP", "resolution_action_updated_date cast; capped at extract time if it lies in the future"),
    ("agency", "STRING", "Responding agency code, upper-cased"),
    ("agency_name", "STRING", "Responding agency full name"),
    ("complaint_type", "STRING", "Problem category"),
    ("descriptor", "STRING", "Problem detail"),
    ("descriptor_2", "STRING", "Additional details"),
    ("location_type", "STRING", "Type of location"),
    ("status", "STRING", "Open, Assigned, In Progress, Pending, Unspecified or Closed (kept as received)"),
    ("borough", "STRING", "Borough, upper-cased"),
    ("incident_zip", "STRING", "5-digit zip code, NULL if not a valid zip"),
    ("city", "STRING", "City name"),
    ("community_board", "STRING", "Community board"),
    ("council_district", "STRING", "Council district"),
    ("police_precinct", "STRING", "Police precinct"),
    ("open_data_channel_type", "STRING", "Channel the request came through"),
    ("latitude", "DOUBLE", "Rounded to 3 decimals (about 110 m); NULL if the source had none"),
    ("longitude", "DOUBLE", "Rounded to 3 decimals; NULL if the source had none"),
    ("is_future_dated", "BOOLEAN", "TRUE if the source update date was later than the extract time"),
    ("row_hash", "STRING", "SHA-256 of the business columns, used to skip no-change updates"),
    ("batch_id", "STRING", "Batch of the version currently stored"),
    ("load_type", "STRING", "FULL or INCREMENTAL"),
    ("extract_timestamp", "TIMESTAMP", "API extraction time of the stored version (orders versions in MERGE)"),
    ("bronze_load_timestamp", "TIMESTAMP", "load_timestamp of the Bronze row this version came from"),
    ("load_timestamp", "TIMESTAMP", "When this version was written to Silver"),
]
SILVER_COLUMN_NAMES = [c[0] for c in SILVER_COLUMNS]

# NYC bounding box used to validate coordinates
NYC_LAT_RANGE = (40.40, 41.00)
NYC_LON_RANGE = (-74.35, -73.60)

# COMMAND ----------

def get_param(name, default=""):
    try:
        dbutils.widgets.text(name, str(default))      # noqa: F821 (dbutils exists in Databricks)
        return dbutils.widgets.get(name).strip()      # noqa: F821
    except Exception:
        return str(default)


def parse_ts(value):
    """Accepts 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS' and returns a datetime."""
    value = value.strip().replace(" ", "T")
    if len(value) == 10:
        value += "T00:00:00"
    return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")


def fmt_ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)


def safe_id(value):
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", value or ""):
        raise ValueError(f"Invalid id '{value}'. Use letters, digits, underscore or dash only.")
    return value


def make_batch_id(load_type, window_start, window_end):
    return f"{load_type}_{window_start[:10].replace('-', '')}_{window_end[:10].replace('-', '')}"


def redact(params):
    return {k: ("***" if "token" in k.lower() else v) for k, v in params.items()}

# COMMAND ----------

def resolve_window(load_type, start_date, end_date, lookback_days, watermark):
    """Return (window_start, window_end) as 'YYYY-MM-DDTHH:MM:SS' strings.

    FULL         : start_date and end_date are required (created_date range).
    INCREMENTAL  : start defaults to LEAST(last watermark, now - lookback_days), so a skipped run is
                   recovered automatically. end defaults to now.
    """
    load_type = load_type.upper()
    if load_type == "FULL":
        if not start_date or not end_date:
            raise ValueError("FULL load needs start_date and end_date.")
        start, end = parse_ts(start_date), parse_ts(end_date)
    elif load_type == "INCREMENTAL":
        now = utc_now()
        end = parse_ts(end_date) if end_date else now
        if start_date:
            start = parse_ts(start_date)
        else:
            floor = end - timedelta(days=int(lookback_days))
            start = min(watermark, floor) if watermark else floor
    else:
        raise ValueError("load_type must be FULL or INCREMENTAL.")
    if start >= end:
        raise ValueError(f"Empty window: {fmt_ts(start)} >= {fmt_ts(end)}")
    return fmt_ts(start), fmt_ts(end)


def build_where(load_type, window_start, window_end, scope_start):
    """SoQL $where clause for the window."""
    if load_type.upper() == "FULL":
        return f"created_date >= '{window_start}' AND created_date < '{window_end}'"
    return (f"(created_date >= '{window_start}' OR "
            f"(resolution_action_updated_date >= '{window_start}' "
            f"AND resolution_action_updated_date <= '{window_end}')) "
            f"AND created_date >= '{scope_start}'")

# COMMAND ----------

LOG_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("layer", StringType()),
    StructField("load_type", StringType()),
    StructField("batch_id", StringType()),
    StructField("source_ref", StringType()),
    StructField("parameters", StringType()),
    StructField("start_time", TimestampType()),
    StructField("end_time", TimestampType()),
    StructField("duration_seconds", DoubleType()),
    StructField("status", StringType()),
    StructField("rows_read", LongType()),
    StructField("rows_inserted", LongType()),
    StructField("rows_updated", LongType()),
    StructField("rows_quarantined", LongType()),
    StructField("error_message", StringType()),
    StructField("notes", StringType()),
    StructField("load_timestamp", TimestampType()),
])


class PipelineRun:
    def __init__(self, layer, load_type, batch_id, source_ref, params):
        self.layer, self.load_type, self.batch_id = layer, load_type, batch_id
        self.source_ref, self.params = source_ref, redact(params)
        self.run_id = str(uuid.uuid4())
        self.rows_read = self.rows_inserted = self.rows_updated = self.rows_quarantined = 0
        self.notes = []

    def note(self, text):
        print(text)
        self.notes.append(text)

    def __enter__(self):
        self.start = datetime.now(timezone.utc)
        return self

    def __exit__(self, exc_type, exc, tb):
        end = datetime.now(timezone.utc)
        row = (self.run_id, self.layer, self.load_type, self.batch_id, self.source_ref,
               json.dumps(self.params, default=str), self.start, end,
               round((end - self.start).total_seconds(), 2),
               "Failure" if exc_type else "Success",
               int(self.rows_read), int(self.rows_inserted), int(self.rows_updated),
               int(self.rows_quarantined),
               (f"{exc_type.__name__}: {exc}"[:2000] if exc_type else None),
               " | ".join(self.notes)[:2000] or None, end)
        try:
            spark.createDataFrame([row], LOG_SCHEMA).write.mode("append").saveAsTable(LOG_TBL)  # noqa: F821
        except Exception as log_err:                    # logging must never hide the real error
            print("WARNING: could not write the execution log row:", log_err)
        return False                                    # re-raise the original exception

# COMMAND ----------

def read_watermark():
    rows = spark.sql(f"SELECT value_ts FROM {CONTROL_TBL} WHERE name = 'incremental_watermark'").collect()  # noqa: F821
    return rows[0][0].replace(tzinfo=None) if rows else None


def advance_watermark(new_value):
    """Moves the watermark forward only (never backwards)."""
    current = read_watermark()
    if current is not None and current >= new_value:
        return
    spark.sql(f"""
        MERGE INTO {CONTROL_TBL} t
        USING (SELECT 'incremental_watermark' AS name,
                      TIMESTAMP '{new_value:%Y-%m-%d %H:%M:%S}' AS value_ts,
                      current_timestamp() AS load_timestamp) s
        ON t.name = s.name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")  # noqa: F821

# COMMAND ----------

RETRY_STATUS = {429, 500, 502, 503, 504}


def api_get(params, token="", max_attempts=6, timeout=180):
    headers = {"X-App-Token": token} if token else {}
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(API_URL, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code not in RETRY_STATUS:
                raise RuntimeError(f"API returned {resp.status_code}: {resp.text[:300]}")
            last_error = f"HTTP {resp.status_code}"
        except requests.RequestException as err:
            last_error = str(err)
        time.sleep(min(2 ** attempt, 60))
    raise RuntimeError(f"API failed after {max_attempts} attempts: {last_error}")

# COMMAND ----------

def _blank_to_null(col):
    """SQL fragment: trim and turn empty strings into NULL."""
    return f"nullif(trim({col}), '')"


def cleanse(bronze):
    s = _blank_to_null
    lat_lo, lat_hi = NYC_LAT_RANGE
    lon_lo, lon_hi = NYC_LON_RANGE
    source_cols_sql = ", ".join(f"'{c}'" for c in SOURCE_COLUMNS)

    typed = bronze.select(
        F.expr(s("unique_key")).alias("unique_key"),
        F.expr(f"try_cast({s('created_date')} AS TIMESTAMP)").alias("created_ts"),
        F.expr(f"try_cast({s('closed_date')} AS TIMESTAMP)").alias("closed_ts"),
        F.expr(f"try_cast({s('due_date')} AS TIMESTAMP)").alias("due_ts"),
        F.expr(f"try_cast({s('resolution_action_updated_date')} AS TIMESTAMP)").alias("upd_ts"),
        F.expr(f"try_cast({s('latitude')} AS DOUBLE)").alias("lat"),
        F.expr(f"try_cast({s('longitude')} AS DOUBLE)").alias("lon"),
        F.expr(s("created_date")).alias("raw_created"),
        F.expr(s("closed_date")).alias("raw_closed"),
        F.expr(s("due_date")).alias("raw_due"),
        F.expr(s("resolution_action_updated_date")).alias("raw_upd"),
        F.expr(s("latitude")).alias("raw_lat"),
        F.expr(s("longitude")).alias("raw_lon"),
        F.expr(f"upper({s('agency')})").alias("agency"),
        F.expr(s("agency_name")).alias("agency_name"),
        F.expr(s("complaint_type")).alias("complaint_type"),
        F.expr(s("descriptor")).alias("descriptor"),
        F.expr(s("descriptor_2")).alias("descriptor_2"),
        F.expr(s("location_type")).alias("location_type"),
        F.expr(s("status")).alias("status"),
        F.expr(f"upper({s('borough')})").alias("borough"),
        F.expr(f"CASE WHEN {s('incident_zip')} RLIKE '^[0-9]{{5}}' "
               f"THEN substring({s('incident_zip')}, 1, 5) END").alias("incident_zip"),
        F.expr(s("city")).alias("city"),
        F.expr(s("community_board")).alias("community_board"),
        F.expr(s("council_district")).alias("council_district"),
        F.expr(s("police_precinct")).alias("police_precinct"),
        F.expr(s("open_data_channel_type")).alias("open_data_channel_type"),
        F.expr(f"""CASE WHEN _rescued_data IS NULL THEN FALSE ELSE
                   size(array_intersect(json_object_keys(_rescued_data),
                                        array({source_cols_sql}))) > 0 END""").alias("schema_conflict"),
        "batch_id", "load_type", "extract_timestamp",
        F.col("load_timestamp").alias("bronze_load_timestamp"),
    )

    has_coord = F.col("raw_lat").isNotNull() | F.col("raw_lon").isNotNull()
    bad_coord = has_coord & (
        F.col("lat").isNull() | F.col("lon").isNull()
        | (F.col("lat") == 0) | (F.col("lon") == 0)
        | ~F.col("lat").between(lat_lo, lat_hi) | ~F.col("lon").between(lon_lo, lon_hi))
    bad_numeric = ((F.col("raw_lat").isNotNull() & F.col("lat").isNull())
                   | (F.col("raw_lon").isNotNull() & F.col("lon").isNull()))
    bad_dates = ((F.col("raw_closed").isNotNull() & F.col("closed_ts").isNull())
                 | (F.col("raw_due").isNotNull() & F.col("due_ts").isNull())
                 | (F.col("raw_upd").isNotNull() & F.col("upd_ts").isNull()))

    flagged = (typed
        .withColumn("quarantine_reason",
            F.when(F.col("unique_key").isNull(), "NULL_KEY")
             .when(F.col("created_ts").isNull(), "BAD_CREATED_DATE")
             .when(bad_dates, "BAD_DATE_FORMAT")
             .when(bad_numeric, "BAD_NUMERIC")
             .when(F.col("schema_conflict"), "SCHEMA_CONFLICT")
             .when(bad_coord, "BAD_COORDINATES"))
        .withColumn("is_future_dated", F.coalesce(F.col("upd_ts") > F.col("extract_timestamp"), F.lit(False)))
        .withColumn("resolution_action_updated_ts",
                    F.when(F.col("is_future_dated"), F.col("extract_timestamp")).otherwise(F.col("upd_ts")))
        .withColumn("latitude", F.round("lat", 3))
        .withColumn("longitude", F.round("lon", 3)))

    hash_cols = ["created_ts", "closed_ts", "due_ts", "resolution_action_updated_ts", "agency",
                 "agency_name", "complaint_type", "descriptor", "descriptor_2", "location_type",
                 "status", "borough", "incident_zip", "city", "community_board", "council_district",
                 "police_precinct", "open_data_channel_type", "latitude", "longitude", "is_future_dated"]
    flagged = flagged.withColumn(
        "row_hash",
        F.sha2(F.concat_ws("\u0001", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in hash_cols]), 256))
    return flagged


def to_silver_columns(flagged):
    """Final Silver projection: the exact column order of the Silver table."""
    return (flagged.withColumn("load_timestamp", F.current_timestamp())
                   .select(*SILVER_COLUMN_NAMES))


def to_quarantine(flagged):
    safe_json = F.to_json(F.struct("raw_created", "raw_closed", "raw_due", "raw_upd", "raw_lat",
                                   "raw_lon", "agency", "complaint_type", "status", "borough",
                                   "incident_zip"))
    return (flagged.where(F.col("quarantine_reason").isNotNull())
            .select("unique_key", F.col("quarantine_reason").alias("reason"), "batch_id",
                    "extract_timestamp", safe_json.alias("record_json"),
                    F.current_timestamp().alias("load_timestamp")))


def dedupe_latest(df):
    """One row per unique_key: the version with the newest extract time wins."""
    from pyspark.sql import Window
    w = Window.partitionBy("unique_key").orderBy(
        F.col("extract_timestamp").desc(), F.col("bronze_load_timestamp").desc(),
        F.col("resolution_action_updated_ts").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")