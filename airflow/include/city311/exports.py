"""CSV extracts of the gold layer, for Tableau Public and the dashboard downloads.

Tableau Public cannot connect to Snowflake, so the pipeline publishes flat
CSVs instead. Each export is one SELECT against the gold (and AI) schemas,
written with a header row. Keep queries warehouse-neutral: they run on
Snowflake in production and on DuckDB in demo mode.

The request-level extract is limited to recent days so the files stay well
inside Tableau Public's limits.
"""
from __future__ import annotations

import csv
import io
from collections.abc import Callable, Iterable
from datetime import date, datetime
from typing import Any

FACT_DAYS = 90


def export_queries(gold: str, ai: str | None = None) -> dict[str, str]:
    q = {
        "city_kpis": f"select * from {gold}.mart_city_kpis order by city",
        "daily_city_volume": f"select * from {gold}.mart_daily_city_volume order by activity_date, city",
        "category_performance": f"select * from {gold}.mart_category_performance order by city, requests desc",
        "open_backlog": f"select * from {gold}.mart_open_backlog order by city, open_requests desc",
        "volume_anomalies": f"""
            select activity_date, series_type, city, series_value, request_count, baseline_mean,
                   z_score, pct_change_vs_baseline, direction
            from {gold}.mart_volume_anomalies
            where is_anomaly
            order by activity_date desc, abs(z_score) desc""",
        "service_requests_recent": f"""
            select request_key, city, source_system, created_date, closed_date, status_group,
                   standard_category, category_raw, channel, area, zip_code, latitude, longitude,
                   is_on_time, resolution_hours
            from {gold}.fct_service_requests
            where created_date > (select max(created_date) from {gold}.fct_service_requests) - {FACT_DAYS}
            order by created_date, request_key""",
    }
    if ai:
        q["ai_daily_insights"] = f"""
            select as_of_date, generated_at, model, severity, headline, summary, anomaly_count
            from {ai}.daily_insights
            order by generated_at desc"""
    return q


def _fmt(v: Any) -> Any:
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, bool):
        return str(v).lower()
    return v


def rows_to_csv(columns: Iterable[str], rows: Iterable[Iterable[Any]]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow([c.lower() for c in columns])
    for r in rows:
        w.writerow([_fmt(v) for v in r])
    return buf.getvalue()


CursorQuery = Callable[[str], tuple[list[str], list[tuple]]]


def build_exports(query: CursorQuery, gold: str, ai: str | None = None) -> dict[str, str]:
    """Return {file_name.csv: csv_text}. A missing AI table is skipped, not fatal."""
    out = {}
    for name, sql in export_queries(gold, ai).items():
        try:
            cols, rows = query(sql)
        except Exception as exc:  # noqa: BLE001 - AI table may not exist yet
            if name == "ai_daily_insights":
                print(f"skipping {name}: {str(exc).splitlines()[0]}")
                continue
            raise
        out[f"{name}.csv"] = rows_to_csv(cols, rows)
    return out
