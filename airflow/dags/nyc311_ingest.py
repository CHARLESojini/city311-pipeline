"""NYC 311 daily ingestion: extract one NYC-local day to S3, then COPY it into Snowflake RAW.

Each run covers the full NYC-local day *before* its logical date, so a run
for 2026-09-25 extracts records created on 2026-09-24. Re-running a date
overwrites the same S3 key; duplicates from reloads are resolved in silver.
"""
import json
from datetime import timedelta

import pendulum
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, get_current_context, task

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
BUCKET = "nyc311-raw-chima"
S3_PREFIX = "landing/nyc"
STAGE = "NYC311.RAW.NYC311_S3_STAGE"
TARGET_TABLE = "NYC311.RAW.SERVICE_REQUESTS"
NYC_TZ = "America/New_York"
TS_FMT = "%Y-%m-%dT%H:%M:%S"


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    tags=["nyc311", "ingestion"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def nyc311_ingest():
    @task
    def extract_to_s3() -> str | None:
        ctx = get_current_context()
        anchor = ctx.get("logical_date") or pendulum.now("UTC")
        window_end = pendulum.datetime(anchor.year, anchor.month, anchor.day, tz=NYC_TZ)
        window_start = window_end.subtract(days=1)

        params = {
            "$where": (
                f"created_date >= '{window_start.strftime(TS_FMT)}' "
                f"AND created_date < '{window_end.strftime(TS_FMT)}'"
            ),
            "$order": "created_date",
            "$limit": 50000,
        }
        resp = requests.get(API_URL, params=params, timeout=120)
        resp.raise_for_status()
        records = resp.json()

        if not records:
            print(f"No records for {window_start:%Y-%m-%d}; nothing written.")
            return None

        key = f"{S3_PREFIX}/created_date={window_start:%Y-%m-%d}/nyc311_{window_start:%Y%m%d}.json"
        S3Hook(aws_conn_id=None).load_string(
            json.dumps(records), key=key, bucket_name=BUCKET, replace=True
        )
        print(f"Wrote {len(records)} records to s3://{BUCKET}/{key}")
        return key

    @task
    def load_to_snowflake(key: str | None) -> None:
        if key is None:
            print("Nothing extracted; skipping load.")
            return

        stage_path = key.removeprefix("landing/")
        sql = f"""
            COPY INTO {TARGET_TABLE} (raw_data, source_file)
            FROM (SELECT $1, METADATA$FILENAME FROM @{STAGE}/{stage_path})
        """
        for row in SnowflakeHook(snowflake_conn_id="snowflake_default").get_records(sql):
            print(row)

    load_to_snowflake(extract_to_s3())


nyc311_ingest()
