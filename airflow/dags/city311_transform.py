"""City 311 transform: dbt bronze -> silver -> gold, triggered by fresh RAW data.

Runs whenever ``city311_ingest`` updates the ``city311_raw`` asset:

    source_freshness   dbt source freshness (a stale feed is reported, not fatal)
    dbt_build          dbt build --target prod: seeds, models and all tests
    publish_gold       updates the ``city311_gold`` asset for downstream consumers
    ai_insights        Claude explains flagged anomalies and dbt test failures;
                       report stored in NYC311.AI.DAILY_INSIGHTS and posted to Slack.
                       Runs even when dbt_build fails, so failures get explained.

dbt runs from its own virtualenv inside the image (see airflow/Dockerfile) so its
dependencies never clash with Airflow's. Snowflake credentials come from the
environment (airflow/.env, see .env.example); nothing is hard-coded here.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

from include.city311.assets import GOLD_ASSET, RAW_ASSET

DBT_BIN = os.getenv("DBT_BIN", "/usr/local/airflow/dbt_venv/bin/dbt")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
DBT_TARGET = os.getenv("DBT_TARGET", "prod")
GOLD_SCHEMA = "NYC311.GOLD"
INSIGHTS_TABLE = "NYC311.AI.DAILY_INSIGHTS"

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

    @task(trigger_rule="all_done", retries=1)
    def ai_insights() -> str:
        from include.city311 import insights as ins

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        def query(sql: str) -> list[dict]:
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql)
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

        ctx = ins.load_context(query, GOLD_SCHEMA, f"{DBT_PROJECT_DIR}/target")
        print(f"as_of={ctx.as_of_date} anomalies={len(ctx.anomalies)} "
              f"test_issues={len(ctx.test_issues)} stale_sources={len(ctx.stale_sources)}")

        client = None
        if ctx.has_signal:
            if not os.getenv("ANTHROPIC_API_KEY"):
                print("Signals found but ANTHROPIC_API_KEY is not set; skipping AI report.")
                return "skipped: no api key"
            import anthropic

            client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from the environment

        insight = ins.generate_insight(ctx, client, os.getenv("CITY311_AI_MODEL", ins.DEFAULT_MODEL))
        row = ins.insight_record(insight, ctx)
        cols = list(row)
        select = ", ".join(
            f"parse_json(%({c})s)" if c in ("insight", "context") else f"%({c})s" for c in cols
        )
        hook.run(f"insert into {INSIGHTS_TABLE} ({', '.join(cols)}) select {select}", parameters=row)

        posted = ins.post_to_slack(ins.slack_message(insight, ctx, os.getenv("CITY311_DASHBOARD_URL")))
        print(f"[{insight['severity']}] {insight['headline']} (slack posted: {posted})")
        return insight["severity"]

    built = dbt_build()
    source_freshness() >> built >> publish_gold()
    built >> ai_insights()


city311_transform()
