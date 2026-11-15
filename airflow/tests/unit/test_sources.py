"""Unit tests for include/city311/sources.py. No Airflow, no network."""
from __future__ import annotations

import pendulum
import pytest

from include.city311 import sources as s


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeSession:
    """Serves pre-baked pages in order and records the params it was called with."""

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(params)
        return FakeResponse(self.pages.pop(0))


# ------------------------------------------------------------------ window
def test_window_is_local_midnight_with_lookback():
    anchor = pendulum.datetime(2026, 11, 14, 3, 0, tz="UTC")  # 22:00 on the 13th in NY
    w = s.extract_window(anchor, lookback_days=3)
    # Anchored on the logical date's calendar day, not on the local clock time
    assert w.bounds() == ("2026-11-11T00:00:00", "2026-11-14T00:00:00")
    assert w.end.timezone_name == "America/New_York"
    assert str(w.as_of) == "2026-11-13"


def test_window_crosses_dst_boundary_cleanly():
    # US DST ended 2026-11-01; local midnights must stay midnights
    w = s.extract_window(pendulum.datetime(2026, 11, 2, tz="UTC"), lookback_days=2)
    assert w.bounds() == ("2026-10-31T00:00:00", "2026-11-02T00:00:00")


def test_window_rejects_zero_lookback():
    with pytest.raises(ValueError):
        s.extract_window(pendulum.datetime(2026, 11, 14, tz="UTC"), lookback_days=0)


# ------------------------------------------------------------------ keys / SQL
@pytest.fixture
def window():
    return s.extract_window(pendulum.datetime(2026, 11, 14, tz="UTC"), lookback_days=3)


def test_s3_key_layout(window):
    assert s.SOURCES_BY_NAME["boston_new"].s3_key(window) == (
        "landing/boston_new/run_date=2026-11-13/boston_new_20261113.json"
    )


def test_copy_into_targets_source_table(window):
    src = s.SOURCES_BY_NAME["nyc"]
    sql = s.copy_into_sql(src, src.s3_key(window), "DB.RAW.STAGE")
    assert "COPY INTO NYC311.RAW.NYC_SERVICE_REQUESTS" in sql
    assert "@DB.RAW.STAGE/nyc/run_date=2026-11-13/nyc_20261113.json" in sql


def test_filters_capture_created_or_closed(window):
    where = s.socrata_where(s.SOURCES_BY_NAME["nyc"], window)
    assert "created_date >= '2026-11-11T00:00:00'" in where
    assert "closed_date < '2026-11-14T00:00:00'" in where
    assert ") OR (" in where

    sql = s.ckan_sql(s.SOURCES_BY_NAME["boston_legacy"], window, 10, 20)
    assert '"1a0b420d-99f1-4887-9851-990b2a5a6e17"' in sql
    assert "open_dt >=" in sql and "closed_dt <" in sql
    assert sql.endswith("ORDER BY _id LIMIT 10 OFFSET 20")


def test_source_registry_is_consistent():
    names = [x.name for x in s.SOURCES]
    assert names == ["nyc", "boston_legacy", "boston_new"]
    assert len({x.raw_table for x in s.SOURCES}) == len(s.SOURCES)
    for x in s.SOURCES:
        if x.fetch is s.fetch_ckan:
            assert x.resource_id, f"{x.name} needs a CKAN resource id"


# ------------------------------------------------------------------ paging
def test_socrata_paginates_until_short_page(monkeypatch, window):
    monkeypatch.setattr(s, "SOCRATA_PAGE_SIZE", 2)
    sess = FakeSession([[{"unique_key": "1"}, {"unique_key": "2"}], [{"unique_key": "3"}]])
    out = s.fetch_socrata(s.SOURCES_BY_NAME["nyc"], window, sess)
    assert [r["unique_key"] for r in out] == ["1", "2", "3"]
    assert [c["$offset"] for c in sess.calls] == [0, 2]
    assert all(c["$order"] == "unique_key" for c in sess.calls)


def test_socrata_exact_multiple_ends_on_empty_page(monkeypatch, window):
    monkeypatch.setattr(s, "SOCRATA_PAGE_SIZE", 2)
    sess = FakeSession([[{"unique_key": "1"}, {"unique_key": "2"}], []])
    assert len(s.fetch_socrata(s.SOURCES_BY_NAME["nyc"], window, sess)) == 2


def test_ckan_paginates_and_strips_internal_fields(monkeypatch, window):
    monkeypatch.setattr(s, "CKAN_PAGE_SIZE", 1)

    def page(rec):
        return {"success": True, "result": {"records": rec}}

    sess = FakeSession(
        [
            page([{"_id": 1, "_full_text": "x", "case_id": "BCS-1"}]),
            page([{"_id": 2, "case_id": "BCS-2"}]),
            page([]),
        ]
    )
    out = s.fetch_ckan(s.SOURCES_BY_NAME["boston_new"], window, sess)
    assert out == [{"case_id": "BCS-1"}, {"case_id": "BCS-2"}]
    assert "OFFSET 2" in sess.calls[-1]["sql"]


def test_ckan_error_raises(window):
    sess = FakeSession([{"success": False, "error": {"message": "bad sql"}}])
    with pytest.raises(RuntimeError, match="bad sql"):
        s.fetch_ckan(s.SOURCES_BY_NAME["boston_new"], window, sess)
