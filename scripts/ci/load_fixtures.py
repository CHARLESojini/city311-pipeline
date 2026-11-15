"""Load deterministic fixture data into a DuckDB file shaped like Snowflake RAW.

Lets CI (and anyone without a Snowflake account) run the full dbt project:

    python scripts/ci/load_fixtures.py dbt/target/city311_ci.duckdb
    cd dbt && DBT_TARGET=ci dbt build

Records copy the field names and value formats of the real APIs (NYC Socrata,
Analyze Boston CKAN) and plant the situations the models and tests must handle:

* the same request landing twice: open in one file, closed in a later one
* NYC closures stamped before creation, and 1900-01-01 placeholders
* Boston's HTML-escaped addresses (&amp;) and numeric ZIP codes (2135)
* the Boston CRM migration: legacy volume fades out as the new system ramps up
* one Boston day where both feeds go nearly silent (an outage to detect)
* one NYC noise spike (a holiday-weekend style surge)
* categories missing from the seed mapping, and junk coordinates
"""
from __future__ import annotations

import json
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

AS_OF = date(2026, 11, 13)
DAYS = 75
SEED = 311

NYC_TYPES = [
    ("Illegal Parking", "NYPD", 30), ("Noise - Residential", "NYPD", 18), ("HEAT/HOT WATER", "HPD", 12),
    ("Street Condition", "DOT", 8), ("Blocked Driveway", "NYPD", 8), ("Dirty Condition", "DSNY", 6),
    ("Street Light Condition", "DOT", 4), ("Rodent", "DOHMH", 4), ("Illegal Dumping", "DSNY", 3),
    ("Graffiti", "DSNY", 2), ("Taxi Complaint", "TLC", 2),  # last one is deliberately unmapped
]
NYC_BOROUGHS = ["BROOKLYN", "QUEENS", "MANHATTAN", "BRONX", "STATEN ISLAND"]
NYC_CHANNELS = ["ONLINE", "PHONE", "MOBILE", "UNKNOWN"]

BOS_LEGACY_TYPES = [
    ("Parking Enforcement", "Enforcement & Abandoned Vehicles", "BTDT", 30),
    ("Requests for Street Cleaning", "Street Cleaning", "PWDx", 15),
    ("Request for Pothole Repair", "Highway Maintenance", "PWDx", 10),
    ("Rodent Activity", "Environmental Services", "ISD", 6),
    ("Pick up Dead Animal", "Street Cleaning", "PWDx", 4),
]
BOS_NEW_TYPES = [
    ("Litter & Debris", "Public Works Department (PWD)", 25),
    ("Illegal Parking", "Boston Transportation Department (BTD)", 15),
    ("Street Light Outage", "Public Works Department (PWD)", 10),
    ("Pothole", "Public Works Department (PWD)", 8),
    ("Domestic Animal Issue", "Animal Care & Control", 5),  # unmapped -> Other
]
BOS_HOODS = [("Dorchester", 2125), ("Jamaica Plain", 2130), ("Allston / Brighton", 2135), ("South End", 2116)]


def ts(dt: datetime, millis: bool = False) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + (".000" if millis else "")


def weighted(rng: random.Random, items):
    return rng.choices(items, weights=[i[-1] for i in items])[0]


def nyc_day_volume(day: date, rng: random.Random) -> int:
    base = 160 if day.weekday() < 5 else 120
    return int(rng.gauss(base, 10))


def build(rng: random.Random) -> dict[str, list[tuple[dict, str, datetime]]]:
    """Return {source: [(record, source_file, loaded_at), ...]}."""
    out: dict[str, list] = {"nyc": [], "boston_legacy": [], "boston_new": []}
    start = AS_OF - timedelta(days=DAYS - 1)
    nyc_id, legacy_id, new_id = 71_000_000, 101_006_000_000, 370_000
    spike_day = AS_OF - timedelta(days=1)
    outage_day = AS_OF - timedelta(days=4)
    migration_start = start + timedelta(days=20)

    for i in range(DAYS):
        day = start + timedelta(days=i)
        def run_file(src: str, day: date = day) -> str:
            return f"{src}/run_date={day:%Y-%m-%d}/{src}_{day:%Y%m%d}.json"

        load_ts = datetime.combine(day + timedelta(days=1), datetime.min.time()) + timedelta(hours=6)

        # ---------------- NYC
        n = nyc_day_volume(day, rng)
        for _ in range(n):
            nyc_id += 1
            ctype, agency, _w = weighted(rng, NYC_TYPES)
            created = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randrange(86_400))
            rec = {
                "unique_key": str(nyc_id), "created_date": ts(created, True), "agency": agency,
                "complaint_type": ctype, "descriptor": "N/A", "status": "In Progress",
                "borough": rng.choice(NYC_BOROUGHS), "incident_zip": str(rng.randint(10001, 11697)),
                "open_data_channel_type": rng.choice(NYC_CHANNELS),
                "latitude": f"{rng.uniform(40.5, 40.9):.6f}", "longitude": f"{rng.uniform(-74.2, -73.7):.6f}",
            }
            if rng.random() < 0.01:
                rec["latitude"], rec["longitude"] = "0", "0"  # junk coordinates
            out["nyc"].append((rec, run_file("nyc"), load_ts))
            # most requests close within a few days; the closure arrives in a later file
            if rng.random() < 0.85:
                closed = created + timedelta(hours=rng.expovariate(1 / 30))
                if rng.random() < 0.01:
                    closed = datetime(1900, 1, 1)  # placeholder seen in the real feed
                if closed.date() <= AS_OF:
                    later = dict(rec, status="Closed", closed_date=ts(closed, True),
                                 resolution_description="The Department addressed the condition.")
                    cday = max(closed.date(), day)
                    out["nyc"].append((later, f"nyc/run_date={cday:%Y-%m-%d}/nyc_{cday:%Y%m%d}.json",
                                       load_ts + timedelta(days=(cday - day).days, minutes=5)))
        if day == spike_day:
            for _ in range(140):  # noise surge
                nyc_id += 1
                created = datetime.combine(day, datetime.min.time()) + timedelta(hours=rng.uniform(20, 24))
                rec = {"unique_key": str(nyc_id), "created_date": ts(created, True), "agency": "NYPD",
                       "complaint_type": "Noise - Street/Sidewalk", "status": "In Progress",
                       "borough": "BROOKLYN", "open_data_channel_type": "MOBILE"}
                out["nyc"].append((rec, run_file("nyc"), load_ts))

        # ---------------- Boston: migration from legacy to new CRM
        progress = min(max((day - migration_start).days / 40, 0), 1)  # 0 -> 1 over 40 days
        total = int(rng.gauss(60 if day.weekday() < 5 else 40, 5))
        if day == outage_day:
            total = 3
        n_new = int(total * progress)
        for _ in range(total - n_new):
            legacy_id += 1
            ctype, reason, dept, _w = weighted(rng, BOS_LEGACY_TYPES)
            hood, zipc = rng.choice(BOS_HOODS)
            created = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randrange(86_400))
            closed = created + timedelta(hours=rng.expovariate(1 / 40))
            is_closed = rng.random() < 0.8 and closed.date() <= AS_OF
            due = created + timedelta(days=2)
            rec = {
                "case_enquiry_id": legacy_id, "open_dt": ts(created), "sla_target_dt": ts(due),
                "closed_dt": ts(closed) if is_closed else None,
                "on_time": "ONTIME" if (closed if is_closed else datetime.combine(AS_OF, datetime.min.time())) <= due else "OVERDUE",
                "case_status": "Closed" if is_closed else "Open", "type": ctype, "reason": reason,
                "department": dept, "source": rng.choice(["Constituent Call", "Citizens Connect App", "Employee Generated"]),
                "neighborhood": hood, "location_zipcode": zipc,
                "location": f"INTERSECTION of Armstrong St &amp; Ashley St  {hood}  MA",
                "latitude": rng.uniform(42.28, 42.37), "longitude": rng.uniform(-71.15, -71.03),
            }
            out["boston_legacy"].append((rec, run_file("boston_legacy"), load_ts))
        for _ in range(n_new):
            new_id += 1
            service, dept, _w = weighted(rng, BOS_NEW_TYPES)
            hood, zipc = rng.choice(BOS_HOODS)
            created = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randrange(86_400))
            due = created + timedelta(days=3)
            rec = {
                "case_id": f"BCS-{new_id:08d}", "open_date": ts(created), "target_close_date": ts(due),
                "close_date": None, "on_time": "ONTIME", "case_status": rng.choice(["In progress", "Create"]),
                "case_topic": service, "service_name": service, "assigned_department": dept,
                "report_source": rng.choice(["BOS311", "Call"]), "neighborhood": hood, "zip_code": zipc,
                "full_address": "1185 Adams St, Boston, MA 02124",
                "latitude": rng.uniform(42.28, 42.37), "longitude": rng.uniform(-71.15, -71.03),
            }
            out["boston_new"].append((rec, run_file("boston_new"), load_ts))
            closed = created + timedelta(hours=rng.expovariate(1 / 50))
            if rng.random() < 0.75 and closed.date() <= AS_OF:
                later = dict(rec, case_status="Closed", close_date=ts(closed),
                             on_time="ONTIME" if closed <= due else "OVERDUE",
                             closure_reason="Case Resolved")
                cday = max(closed.date(), day)
                out["boston_new"].append((later, f"boston_new/run_date={cday:%Y-%m-%d}/boston_new_{cday:%Y%m%d}.json",
                                          load_ts + timedelta(days=(cday - day).days, minutes=5)))

    # NYC closure before creation (data-entry error in the real feed)
    bad = dict(out["nyc"][0][0], status="Closed", closed_date="2026-08-01T00:00:00.000",
               created_date="2026-09-01T10:00:00.000")
    out["nyc"].append((bad, out["nyc"][0][1], out["nyc"][0][2] + timedelta(hours=1)))
    return out


TABLES = {
    "nyc": "NYC_SERVICE_REQUESTS",
    "boston_legacy": "BOSTON_LEGACY_SERVICE_REQUESTS",
    "boston_new": "BOSTON_NEW_SERVICE_REQUESTS",
}


def main(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    data = build(random.Random(SEED))
    con = duckdb.connect(db_path)
    con.execute("create schema if not exists RAW")
    for src, table in TABLES.items():
        con.execute(f"create or replace table RAW.{table} (raw_data json, source_file varchar, loaded_at timestamp)")
        con.executemany(
            f"insert into RAW.{table} values (?, ?, ?)",
            [(json.dumps(r), f, t) for r, f, t in data[src]],
        )
        print(f"RAW.{table}: {len(data[src]):,} rows")
    con.close()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "dbt/target/city311_ci.duckdb")
