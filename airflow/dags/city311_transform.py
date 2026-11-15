"""City 311 transform: dbt bronze -> silver -> gold, triggered by fresh RAW data.

Runs whenever ``city311_ingest`` updates the ``city311_raw`` asset:

    source_freshness   dbt source freshness (a stale feed is reported, not fatal)
    dbt_build          dbt build --target prod: seeds, models and all tests
    publish_gold       updates the ``city311_gold`` asset for downstream consumers

dbt runs from its own virtualenv inside the image (see airflow/Dockerfile) so its
dependencies never clash with Airflow's. Snowflake credentials come from the
environment (airflow/.env, see .env.example); nothing is hard-coded here.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from include.city311.assets import GOLD_ASSET, RAW_ASSET

DBT_BIN = os.getenv("DBT_BIN", "/usr/local/airflow/dbt_venv/bin/dbt")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
DBT_TARGET = os.getenv("DBT_TARGET", "prod")

DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    "DBT_TARGET": DBT_TARGET,
    # keep artifacts (run_results.json) where the AI layer can read them
    "DBT_TARGET_PATH": f"{DBT_PROJECT_DIR}/target",
    "DBT_LOG_PATH": f"{DBT_PROJECT_DIR}/logs",
}


def dbt(cmd: str) -> str:
    return f"cd {DBT_PROJECT_DIR} && {DBT_BIN} {cmd} --target {DBT_TARGET}"


@dag(
    schedule=[RAW_ASSET],
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["city311", "transform", "dbt"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    doc_md=__doc__,
)
def city311_transform():
    @task.bash(env=DBT_ENV, append_env=True)
    def source_freshness() -> str:
        # Exit 0 even on stale sources: freshness is surfaced in logs and to the
        # AI layer, while the build still runs on whatever data did arrive.
        return dbt("source freshness") + " || echo 'WARNING: at least one source is stale'"

    @task.bash(env=DBT_ENV, append_env=True, retries=0)
    def dbt_build() -> str:
        # No retries: a failing data test will fail again; retrying only delays the alert.
        return dbt("build")

    @task(outlets=[GOLD_ASSET])
    def publish_gold() -> None:
        print("Gold marts rebuilt.")

    source_freshness() >> dbt_build() >> publish_gold()


city311_transform()
