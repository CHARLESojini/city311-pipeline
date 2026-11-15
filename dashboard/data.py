"""Data access for the dashboard: Snowflake when configured, DuckDB demo otherwise.

Live mode (Streamlit Community Cloud or local): add a [snowflake] section to
.streamlit/secrets.toml (see secrets.toml.example). The private key is read
from secrets into memory; it is never written to disk.

Demo mode: with no Snowflake secrets, the app reads the DuckDB warehouse that
CI builds from fixture data (scripts/ci/load_fixtures.py + dbt build --target ci).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DEMO_DB = os.getenv("CITY311_DEMO_DB", str(ROOT / "dbt/target/city311_ci.duckdb"))


@dataclass(frozen=True)
class Backend:
    name: str  # "snowflake" | "duckdb"
    gold: str
    ai: str


def _snowflake_secrets() -> dict | None:
    try:
        return dict(st.secrets["snowflake"])
    except (KeyError, FileNotFoundError, st.errors.StreamlitSecretNotFoundError):
        return None


def backend() -> Backend:
    if _snowflake_secrets():
        return Backend("snowflake", "NYC311.GOLD", "NYC311.AI")
    return Backend("duckdb", "main_gold", "ai")


@st.cache_resource(show_spinner=False)
def _snowflake_connection():
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    cfg = _snowflake_secrets()
    key = serialization.load_pem_private_key(
        cfg["private_key"].encode(),
        password=cfg["private_key_passphrase"].encode() if cfg.get("private_key_passphrase") else None,
    )
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        private_key=key,
        role=cfg.get("role", "REPORTER"),
        warehouse=cfg.get("warehouse", "NYC311_WH"),
        database=cfg.get("database", "NYC311"),
        client_session_keep_alive=True,
        session_parameters={"QUERY_TAG": "city311_dashboard"},
    )


@st.cache_resource(show_spinner=False)
def _duckdb_connection():
    import duckdb

    if not Path(DEMO_DB).exists():
        raise FileNotFoundError(
            f"Demo warehouse not found at {DEMO_DB}. Build it with:\n"
            "  python scripts/ci/load_fixtures.py dbt/target/city311_ci.duckdb\n"
            "  cd dbt && DBT_PROFILES_DIR=. dbt build --target ci"
        )
    return duckdb.connect(DEMO_DB, read_only=True)


@st.cache_data(ttl=3600, show_spinner=False)
def query(sql: str) -> pd.DataFrame:
    """Run a read-only query; results are cached for an hour (gold refreshes daily)."""
    if backend().name == "snowflake":
        cur = _snowflake_connection().cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
    else:
        df = _duckdb_connection().execute(sql).df()
    df.columns = [c.lower() for c in df.columns]
    return df


def table_exists(schema: str, table: str) -> bool:
    try:
        query(f"select 1 from {schema}.{table} limit 1")
        return True
    except Exception:  # noqa: BLE001
        return False
