from datetime import date

import pytest

from include.city311.exports import build_exports, export_queries, rows_to_csv


def test_csv_lowercases_headers_and_formats_values():
    text = rows_to_csv(["CITY", "AS_OF_DATE", "IS_ON_TIME", "N"], [("NYC", date(2026, 11, 13), True, 5)])
    assert text == "city,as_of_date,is_on_time,n\nNYC,2026-11-13,true,5\n"


def test_queries_target_the_given_schemas():
    q = export_queries("NYC311.GOLD", "NYC311.AI")
    assert set(q) >= {"city_kpis", "daily_city_volume", "service_requests_recent", "ai_daily_insights"}
    assert all("NYC311.GOLD." in sql for name, sql in q.items() if name != "ai_daily_insights")
    assert "ai_daily_insights" not in export_queries("g")


def test_missing_ai_table_is_skipped_but_gold_errors_raise():
    def query(sql):
        if "daily_insights" in sql:
            raise RuntimeError("does not exist")
        return ["c"], [(1,)]

    out = build_exports(query, "g", "ai")
    assert "ai_daily_insights.csv" not in out and out["city_kpis.csv"] == "c\n1\n"

    def broken(sql):
        raise RuntimeError("warehouse down")

    with pytest.raises(RuntimeError):
        build_exports(broken, "g")
