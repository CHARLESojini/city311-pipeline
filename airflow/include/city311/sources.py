"""Source definitions and extract logic for every 311 feed the pipeline ingests.

Three feeds, two API styles:

* ``nyc``            NYC Open Data (Socrata SODA API), dataset erm2-nwe9
* ``boston_legacy``  Analyze Boston (CKAN datastore), legacy CRM, 12-digit case IDs
* ``boston_new``     Analyze Boston (CKAN datastore), new CRM (since Oct 2025), BCS- case IDs

Extract window
--------------
311 records are *revised in place*: a request is created, then closed days
later. Pulling only by created date would never capture closures, so every
extract selects records that were **created OR closed** inside the window.
The window also looks back a few days, because both portals publish with a
lag of a day or more. Overlapping windows produce duplicate versions of the
same request; silver keeps the latest version (see dbt ``slv_*`` models).

All timestamps in both portals are local (America/New_York) wall-clock
values with no offset, so the window is computed in that zone.
"""
from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

import pendulum
import requests

LOCAL_TZ = "America/New_York"
TS_FMT = "%Y-%m-%dT%H:%M:%S"
DEFAULT_LOOKBACK_DAYS = 3

SOCRATA_PAGE_SIZE = 50_000
CKAN_PAGE_SIZE = 10_000  # CKAN caps a datastore_search_sql response at 32k rows
CKAN_INTERNAL_FIELDS = ("_id", "_full_text")


@dataclass(frozen=True)
class Window:
    """Half-open local-time window [start, end)."""

    start: pendulum.DateTime
    end: pendulum.DateTime

    @property
    def as_of(self) -> pendulum.Date:
        """Last calendar day fully covered by the window; used for partitioning."""
        return self.end.subtract(days=1).date()

    def bounds(self) -> tuple[str, str]:
        return self.start.strftime(TS_FMT), self.end.strftime(TS_FMT)


def extract_window(anchor: pendulum.DateTime, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> Window:
    """Window ending at local midnight of the anchor's date.

    A run whose logical date is 2026-11-14 covers local days
    2026-11-11 .. 2026-11-13 with the default 3-day lookback.
    """
    if lookback_days < 1:
        raise ValueError("lookback_days must be >= 1")
    end = pendulum.datetime(anchor.year, anchor.month, anchor.day, tz=LOCAL_TZ)
    return Window(start=end.subtract(days=lookback_days), end=end)


@dataclass(frozen=True)
class Source:
    name: str
    city: str
    raw_table: str
    fetch: Callable[[Source, Window, requests.Session], list[dict[str, Any]]]
    url: str
    created_field: str
    closed_field: str
    id_field: str
    resource_id: str | None = None

    def s3_key(self, window: Window, prefix: str = "landing") -> str:
        day = window.as_of
        return f"{prefix}/{self.name}/run_date={day:%Y-%m-%d}/{self.name}_{day:%Y%m%d}.json"


# --------------------------------------------------------------------------- Socrata
def socrata_where(src: Source, window: Window) -> str:
    start, end = window.bounds()
    return (
        f"({src.created_field} >= '{start}' AND {src.created_field} < '{end}') "
        f"OR ({src.closed_field} >= '{start}' AND {src.closed_field} < '{end}')"
    )


def fetch_socrata(src: Source, window: Window, session: requests.Session) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for page in _socrata_pages(src, window, session):
        records.extend(page)
    return records


def _socrata_pages(src: Source, window: Window, session: requests.Session) -> Iterator[list[dict]]:
    offset = 0
    while True:
        params = {
            "$where": socrata_where(src, window),
            "$order": src.id_field,  # stable order is required for offset paging
            "$limit": SOCRATA_PAGE_SIZE,
            "$offset": offset,
        }
        resp = session.get(src.url, params=params, timeout=180)
        resp.raise_for_status()
        page = resp.json()
        if page:
            yield page
        if len(page) < SOCRATA_PAGE_SIZE:
            return
        offset += SOCRATA_PAGE_SIZE


# --------------------------------------------------------------------------- CKAN
def ckan_sql(src: Source, window: Window, limit: int, offset: int) -> str:
    start, end = window.bounds()
    return (
        f'SELECT * FROM "{src.resource_id}" '
        f"WHERE ({src.created_field} >= '{start}' AND {src.created_field} < '{end}') "
        f"OR ({src.closed_field} >= '{start}' AND {src.closed_field} < '{end}') "
        f"ORDER BY _id LIMIT {limit} OFFSET {offset}"
    )


def fetch_ckan(src: Source, window: Window, session: requests.Session) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    offset = 0
    while True:
        resp = session.get(
            src.url, params={"sql": ckan_sql(src, window, CKAN_PAGE_SIZE, offset)}, timeout=180
        )
        resp.raise_for_status()
        body = resp.json()
        if not body.get("success"):
            raise RuntimeError(f"CKAN query failed for {src.name}: {body.get('error')}")
        page = body["result"]["records"]
        records.extend(
            {k: v for k, v in rec.items() if k not in CKAN_INTERNAL_FIELDS} for rec in page
        )
        if len(page) < CKAN_PAGE_SIZE:
            return records
        offset += CKAN_PAGE_SIZE


# --------------------------------------------------------------------------- registry
CKAN_SQL_URL = "https://data.boston.gov/api/3/action/datastore_search_sql"

SOURCES: tuple[Source, ...] = (
    Source(
        name="nyc",
        city="NYC",
        raw_table="NYC311.RAW.NYC_SERVICE_REQUESTS",
        fetch=fetch_socrata,
        url="https://data.cityofnewyork.us/resource/erm2-nwe9.json",
        created_field="created_date",
        closed_field="closed_date",
        id_field="unique_key",
    ),
    Source(
        name="boston_legacy",
        city="Boston",
        raw_table="NYC311.RAW.BOSTON_LEGACY_SERVICE_REQUESTS",
        fetch=fetch_ckan,
        url=CKAN_SQL_URL,
        # "311 Service Requests - 2026". Boston publishes one legacy resource per
        # year: add next year's resource id here when it appears (or retire this
        # source once the legacy CRM is fully decommissioned).
        resource_id="1a0b420d-99f1-4887-9851-990b2a5a6e17",
        created_field="open_dt",
        closed_field="closed_dt",
        id_field="case_enquiry_id",
    ),
    Source(
        name="boston_new",
        city="Boston",
        raw_table="NYC311.RAW.BOSTON_NEW_SERVICE_REQUESTS",
        fetch=fetch_ckan,
        url=CKAN_SQL_URL,
        resource_id="254adca6-64ab-4c5c-9fc0-a6da622be185",  # "311 Service Requests - NEW SYSTEM"
        created_field="open_date",
        closed_field="close_date",
        id_field="case_id",
    ),
)

SOURCES_BY_NAME = {s.name: s for s in SOURCES}


def copy_into_sql(src: Source, key: str, stage: str, landing_prefix: str = "landing/") -> str:
    """COPY one landed file into the source's RAW table.

    Snowflake remembers which files it has loaded (by name + checksum), so
    re-running an unchanged file is a no-op, while a re-extracted file with new
    content is loaded again; silver dedupes those versions.
    """
    stage_path = key.removeprefix(landing_prefix)
    return (
        f"COPY INTO {src.raw_table} (raw_data, source_file)\n"
        f"FROM (SELECT $1, METADATA$FILENAME FROM @{stage}/{stage_path})"
    )
