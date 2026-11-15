"""Write the gold tables to CSV for Tableau Public.

    # from Snowflake (reads SNOWFLAKE_* from the environment, see .env.example)
    set -a; source airflow/.env; set +a
    python scripts/export_gold_csv.py --source snowflake

    # from the local DuckDB demo warehouse (no credentials needed)
    python scripts/export_gold_csv.py --source duckdb

Files land in exports/tableau/ (gitignored). The Airflow transform DAG also
publishes the same files to s3://$CITY311_BUCKET/exports/tableau/latest/.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "airflow"))

from include.city311.exports import build_exports  # noqa: E402


def duckdb_query(path: str):
    import duckdb

    con = duckdb.connect(path, read_only=True)

    def query(sql: str):
        cur = con.execute(sql)
        return [d[0] for d in cur.description], cur.fetchall()

    return query


def snowflake_query():
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    key_path = os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])
    private_key = serialization.load_pem_private_key(Path(key_path).read_bytes(), password=None)
    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=private_key,
        authenticator="SNOWFLAKE_JWT",
        role=os.getenv("SNOWFLAKE_ROLE", "REPORTER"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "NYC311_WH"),
        database="NYC311",
    )

    def query(sql: str):
        cur = con.cursor()
        cur.execute(sql)
        return [d[0] for d in cur.description], cur.fetchall()

    return query


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["snowflake", "duckdb"], default="snowflake")
    p.add_argument("--duckdb-path", default=str(ROOT / "dbt/target/city311_ci.duckdb"))
    p.add_argument("--out", default=str(ROOT / "exports/tableau"))
    args = p.parse_args()

    if args.source == "snowflake":
        query, gold, ai = snowflake_query(), "NYC311.GOLD", "NYC311.AI"
    else:
        query, gold, ai = duckdb_query(args.duckdb_path), "main_gold", "ai"

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for name, text in build_exports(query, gold, ai).items():
        (out / name).write_text(text)
        print(f"{out / name}  ({text.count(chr(10)) - 1:,} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
