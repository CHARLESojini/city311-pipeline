"""City 311 daily ingestion: NYC + Boston (legacy and new CRM) -> S3 -> Snowflake RAW.

For each source, in parallel:
    extract_<source>  pull records created OR closed in the lookback window, land
                      one JSON file in s3://<bucket>/landing/<source>/run_date=.../
    load_<source>     COPY that file into the source's own RAW table

When every load succeeds, ``publish_raw`` updates the ``city311_raw`` asset,
which triggers the ``city311_transform`` DAG (dbt + AI insights).

Idempotency: re-running a date overwrites the same S3 key. Duplicate versions
of a request (re-runs, overlapping lookback windows) are resolved in silver.
"""
from __future__ import annotations

import json
import os
from datetime import timedelta

import pendulum
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Asset, dag, get_current_context, task

from include.city311.sources import (
    DEFAULT_LOOKBACK_DAYS,
    SOURCES,
    SOURCES_BY_NAME,
    copy_into_sql,
    extract_window,
)

BUCKET = os.getenv("CITY311_BUCKET", "nyc311-raw-chima")
STAGE = "NYC311.RAW.NYC311_S3_STAGE"
LOOKBACK_DAYS = int(os.getenv("CITY311_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS))

RAW_ASSET = Asset(name="city311_raw")


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["city311", "ingestion"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    doc_md=__doc__,
)
def city311_ingest():
    @task
    def extract(source_name: str) -> str | None:
        src = SOURCES_BY_NAME[source_name]
        ctx = get_current_context()
        anchor = ctx.get("logical_date") or pendulum.now("UTC")
        window = extract_window(anchor, LOOKBACK_DAYS)

        with requests.Session() as session:
            session.headers["User-Agent"] = "city311-pipeline (portfolio project)"
            records = src.fetch(src, window, session)

        if not records:
            print(f"[{src.name}] no records for {window.start} .. {window.end}; nothing written.")
            return None

        key = src.s3_key(window)
        S3Hook(aws_conn_id=None).load_string(
            json.dumps(records), key=key, bucket_name=BUCKET, replace=True
        )
        print(f"[{src.name}] wrote {len(records):,} records to s3://{BUCKET}/{key}")
        return key

    @task
    def load(source_name: str, key: str | None) -> int:
        if key is None:
            print(f"[{source_name}] nothing extracted; skipping load.")
            return 0
        src = SOURCES_BY_NAME[source_name]
        rows = SnowflakeHook(snowflake_conn_id="snowflake_default").get_records(
            copy_into_sql(src, key, STAGE)
        )
        # COPY returns one row per file: (file, status, rows_parsed, rows_loaded, ...)
        loaded = sum(int(r[3]) for r in rows if len(r) > 3 and str(r[3]).isdigit())
        for r in rows:
            print(f"[{source_name}] {r}")
        return loaded

    @task(outlets=[RAW_ASSET])
    def publish_raw(loaded_counts: list[int]) -> None:
        print(f"RAW refreshed; rows loaded this run: {sum(loaded_counts):,}")

    loads = []
    for src in SOURCES:
        key = extract.override(task_id=f"extract_{src.name}")(src.name)
        loads.append(load.override(task_id=f"load_{src.name}")(src.name, key))
    publish_raw(loads)


city311_ingest()
