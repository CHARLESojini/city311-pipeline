"""Run the Streamlit app headless against the DuckDB fixture warehouse and fail on any exception."""
from __future__ import annotations

import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest


def main() -> int:
    at = AppTest.from_file(str(Path(__file__).resolve().parents[2] / "dashboard/app.py"), default_timeout=120).run()
    if at.exception:
        for e in at.exception:
            print(e.value)
        return 1
    metrics = {m.label for m in at.metric}
    expected = {"Requests, last 7 days", "Median hours to close", "Closed on time"}
    if not expected <= metrics:
        print(f"missing KPI tiles: {expected - metrics}")
        return 1
    print(f"dashboard OK: {len(at.subheader)} sections, {len(at.metric)} KPI tiles")
    return 0


if __name__ == "__main__":
    sys.exit(main())
