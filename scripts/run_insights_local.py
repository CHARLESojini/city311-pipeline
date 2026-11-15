"""Run the AI insights layer locally against the DuckDB fixture warehouse.

    python scripts/ci/load_fixtures.py dbt/target/city311_ci.duckdb
    (cd dbt && DBT_PROFILES_DIR=. dbt build --target ci)
    ANTHROPIC_API_KEY=... python scripts/run_insights_local.py

Without ANTHROPIC_API_KEY it prints the exact context Claude would receive and
stops. With it, it calls Claude and stores the report in ai.daily_insights in
the same DuckDB file, which the dashboard's demo mode displays.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "airflow"))

from include.city311 import insights as ins  # noqa: E402

DB = os.getenv("DBT_DUCKDB_PATH", str(ROOT / "dbt/target/city311_ci.duckdb"))


def main() -> int:
    con = duckdb.connect(DB)

    def query(sql: str) -> list[dict]:
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    ctx = ins.load_context(query, "main_gold", str(ROOT / "dbt/target"))
    print(f"as of {ctx.as_of_date}: {len(ctx.anomalies)} anomalies, {len(ctx.test_issues)} test issues")

    if ctx.has_signal and not os.getenv("ANTHROPIC_API_KEY"):
        print("\nANTHROPIC_API_KEY not set. Context Claude would receive:\n")
        print(json.dumps(ctx.as_payload(), indent=1, default=str))
        return 0

    client = None
    if ctx.has_signal:
        import anthropic

        client = anthropic.Anthropic()
    insight = ins.generate_insight(ctx, client, os.getenv("CITY311_AI_MODEL", ins.DEFAULT_MODEL))
    row = ins.insight_record(insight, ctx)

    con.execute("create schema if not exists ai")
    con.execute("""
        create table if not exists ai.daily_insights (
            as_of_date date, generated_at timestamp, model varchar, severity varchar,
            headline varchar, summary varchar, insight json, context json, context_hash varchar,
            anomaly_count integer, test_issue_count integer, dropped_findings integer)
    """)
    con.execute(f"insert into ai.daily_insights ({', '.join(row)}) values ({', '.join('?' * len(row))})",
                list(row.values()))
    print(json.dumps({k: v for k, v in insight.items() if k != "_meta"}, indent=2))
    print(ins.slack_message(insight, ctx)["text"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
