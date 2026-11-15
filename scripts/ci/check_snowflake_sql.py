"""Parse every dbt-compiled model as Snowflake SQL.

CI builds the project on DuckDB, which proves the logic but not the Snowflake
rendering of the cross-db macros. This compiles the prod target offline
(no connection) and parses the output with sqlglot's Snowflake dialect.

    cd dbt && dbt compile --target prod --no-populate-cache --target-path target_sf
    python ../scripts/ci/check_snowflake_sql.py target_sf
"""
from __future__ import annotations

import sys
from pathlib import Path

import sqlglot


def main(target_path: str) -> int:
    files = sorted(Path(target_path, "compiled").rglob("*.sql"))
    models = [f for f in files if "/models/" in f.as_posix()]
    if not models:
        print(f"No compiled models under {target_path}/compiled")
        return 1
    failures = 0
    for f in models:
        try:
            sqlglot.parse_one(f.read_text(), read="snowflake")
        except sqlglot.errors.ParseError as exc:
            failures += 1
            print(f"FAIL {f}: {str(exc).splitlines()[0]}")
    print(f"{len(models) - failures}/{len(models)} models parse as Snowflake SQL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "target_sf"))
