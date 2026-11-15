"""AI insights layer: Claude explains what the pipeline already detected.

Division of labour
------------------
* **SQL detects.** ``mart_volume_anomalies`` flags unusual days with a z-score
  against a trailing baseline, and dbt tests flag data-quality failures. Both
  are deterministic and reproducible.
* **Claude explains.** It receives only those computed facts (never raw rows),
  and returns a structured JSON report: what happened, the likely cause
  (real-world event, pipeline issue, source-system change, publication lag)
  and what to do next.
* **Code verifies.** The response is schema-constrained (structured outputs),
  then validated against the context: findings that cite an anomaly the
  pipeline did not detect are dropped, so the model cannot invent incidents.

When nothing is flagged, no model call is made at all.

This module has no Airflow imports. The DAG (city311_transform) passes in a
query function and an Anthropic client, which keeps it unit-testable.
"""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests

DEFAULT_MODEL = "claude-haiku-5-5"
MAX_ANOMALIES = 15
MAX_FINDINGS = 8
MAX_OUTPUT_TOKENS = 16_000  # headroom for the model's reasoning plus the JSON report
RECENT_DAYS = 3

QueryFn = Callable[[str], list[dict[str, Any]]]

SYSTEM_PROMPT = """\
You are the on-call data analyst for a 311 service-request pipeline covering
New York City and Boston. Each morning you write a short incident report for
the data team from facts the pipeline has already computed. You explain; you
do not detect. Use only the numbers in the context you are given.

Domain knowledge that matters for explanations:
- Boston is migrating 311 from a legacy CRM (12-digit case IDs, source_system
  boston_legacy) to a new CRM (BCS- case IDs, boston_new), one case type at a
  time. A legacy drop together with a new-system rise for the same day is the
  migration, not a change in demand: check the city-level series before calling
  it an incident. In August 2026 the city mistakenly dropped migrated case types
  from the legacy dataset, which looked like a sharp volume fall; it was fixed
  on 2026-08-25.
- Both portals publish with a lag of a day or more. The pipeline therefore
  treats the newest day as provisional and leaves it out of everything you are
  shown: as_of_date is the last complete day. Do not report the missing newer
  day as an incident.
- NYC volume spikes on hot nights and holiday weekends (noise), after storms
  (trees, flooding, street conditions) and during heat season (heat/hot water).
  Boston spikes after snowstorms (snow and ice, unshoveled sidewalks).
- A failing dbt test means the warehouse data cannot be fully trusted until
  fixed; mention which test and what it protects.

Rules:
- Every finding must reference the anomaly_key of a flagged anomaly from the
  context, or "data_quality" for a dbt test/freshness issue.
- Prefer the most plausible cause, give your confidence, and say what would
  confirm it. Do not overstate: "unclear" is an acceptable likely_cause.
- Severity: critical if data is missing or wrong for a whole city or source,
  warning for notable anomalies or test warnings, info otherwise.
- Report at most {max_findings} findings. When several anomalies share one cause
  (for example the Boston CRM migration), merge them into a single finding that
  cites the most significant anomaly_key and mentions the others in the explanation.
- Be concise. The headline is one sentence; each explanation is at most three.
"""

INSIGHT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["headline", "severity", "summary", "findings", "data_quality_notes"],
    "properties": {
        "headline": {"type": "string"},
        "severity": {"type": "string", "enum": ["info", "warning", "critical"]},
        "summary": {"type": "string"},
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "anomaly_key", "city", "title", "explanation",
                    "likely_cause", "confidence", "recommended_action",
                ],
                "properties": {
                    "anomaly_key": {"type": "string"},
                    "city": {"type": "string", "enum": ["NYC", "Boston", "Both"]},
                    "title": {"type": "string"},
                    "explanation": {"type": "string"},
                    "likely_cause": {
                        "type": "string",
                        "enum": [
                            "real_world_event", "data_pipeline_issue",
                            "source_system_change", "publication_lag", "unclear",
                        ],
                    },
                    "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
                    "recommended_action": {"type": "string"},
                },
            },
        },
        "data_quality_notes": {"type": "array", "items": {"type": "string"}},
    },
}


# ---------------------------------------------------------------- context
@dataclass
class InsightContext:
    as_of_date: str
    anomalies: list[dict[str, Any]]
    city_kpis: list[dict[str, Any]]
    recent_volume: list[dict[str, Any]]
    test_issues: list[dict[str, Any]] = field(default_factory=list)
    stale_sources: list[dict[str, Any]] = field(default_factory=list)

    @property
    def anomaly_keys(self) -> set[str]:
        return {a["anomaly_key"] for a in self.anomalies}

    @property
    def has_signal(self) -> bool:
        return bool(self.anomalies or self.test_issues or self.stale_sources)

    def as_payload(self) -> dict[str, Any]:
        return {
            "as_of_date": self.as_of_date,
            "flagged_anomalies": self.anomalies,
            "city_kpis": self.city_kpis,
            "daily_volume_by_source_last_14_days": self.recent_volume,
            "dbt_test_issues": self.test_issues,
            "stale_sources": self.stale_sources,
        }

    def fingerprint(self) -> str:
        blob = json.dumps(self.as_payload(), sort_keys=True, default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:16]


def _clean(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Lower-case keys (Snowflake returns upper case), JSON-safe values, rounded floats."""
    out = []
    for row in rows:
        clean = {}
        for k, v in row.items():
            if isinstance(v, float):
                v = round(v, 3)
            elif isinstance(v, (date, datetime)):
                v = v.isoformat()
            elif v is not None and not isinstance(v, (int, str, bool)):
                v = float(v)  # Decimal and friends
            clean[k.lower()] = v
        out.append(clean)
    return out


def load_context(query: QueryFn, gold: str, dbt_target_dir: str | None = None) -> InsightContext:
    """Collect the facts Claude will explain. `gold` is the schema, e.g. NYC311.GOLD."""
    # Last complete day: the newest day(s) are provisional (partially published) and excluded.
    as_of = _clean(query(
        f"select max(activity_date) as as_of_date from {gold}.mart_volume_anomalies where not is_provisional"
    ))
    as_of_date = as_of[0]["as_of_date"] if as_of and as_of[0]["as_of_date"] else None
    if as_of_date is None:
        return InsightContext(as_of_date="unknown", anomalies=[], city_kpis=[], recent_volume=[])

    anomalies = _clean(query(f"""
        select anomaly_key, activity_date, series_type, city, series_value, direction,
               request_count, baseline_mean, z_score, pct_change_vs_baseline
        from {gold}.mart_volume_anomalies
        where is_anomaly
          and activity_date > cast('{as_of_date}' as date) - {RECENT_DAYS}
        order by abs(z_score) desc
        limit {MAX_ANOMALIES}
    """))
    kpis = _clean(query(f"select * from {gold}.mart_city_kpis order by city"))
    recent = _clean(query(f"""
        select activity_date, city, source_system, sum(opened_count) as opened
        from {gold}.mart_daily_city_volume
        where activity_date > cast('{as_of_date}' as date) - 14
          and activity_date <= cast('{as_of_date}' as date)
        group by 1, 2, 3
        order by 1, 2, 3
    """))
    tests, stale = ([], [])
    if dbt_target_dir:
        tests = read_dbt_test_issues(Path(dbt_target_dir) / "run_results.json")
        stale = read_stale_sources(Path(dbt_target_dir) / "sources.json")
    return InsightContext(str(as_of_date), anomalies, kpis, recent, tests, stale)


def read_dbt_test_issues(run_results: Path) -> list[dict[str, Any]]:
    """Failed / warned / errored dbt nodes from the last `dbt build`."""
    if not run_results.exists():
        return []
    data = json.loads(run_results.read_text())
    issues = []
    for r in data.get("results", []):
        if r.get("status") in ("fail", "warn", "error"):
            issues.append({
                # unique_id is <type>.<package>.<name>[.<hash>]
                "test": (r["unique_id"].split(".") + [""] * 3)[2] or r["unique_id"],
                "status": r["status"],
                "failing_rows": r.get("failures"),
                "message": (r.get("message") or "")[:300],
            })
    return issues


def read_stale_sources(sources_json: Path) -> list[dict[str, Any]]:
    """Sources that warned or errored in the last `dbt source freshness`."""
    if not sources_json.exists():
        return []
    data = json.loads(sources_json.read_text())
    return [
        {"source": r["unique_id"].split(".")[-1], "status": r["status"],
         "hours_since_load": round(r.get("max_loaded_at_time_ago_in_s", 0) / 3600, 1)}
        for r in data.get("results", [])
        if r.get("status") in ("warn", "error", "runtime error")
    ]


# ---------------------------------------------------------------- generation
def all_clear(ctx: InsightContext) -> dict[str, Any]:
    return {
        "headline": f"No anomalies or data-quality issues as of {ctx.as_of_date}.",
        "severity": "info",
        "summary": "Volumes for every city, source system and category are within their normal range, "
                   "and all dbt tests passed.",
        "findings": [],
        "data_quality_notes": [],
    }


def generate_insight(ctx: InsightContext, client: Any | None, model: str = DEFAULT_MODEL) -> dict[str, Any]:
    """Return a validated insight dict. Skips the model call when there is nothing to explain."""
    if not ctx.has_signal:
        return {**all_clear(ctx), "_meta": {"model": None, "dropped_findings": 0}}
    if client is None:
        raise RuntimeError("Anomalies need explaining but no Anthropic client was provided")

    response = client.messages.create(
        model=model,
        max_tokens=MAX_OUTPUT_TOKENS,
        system=SYSTEM_PROMPT.format(max_findings=MAX_FINDINGS),
        messages=[{
            "role": "user",
            "content": "Write today's incident report from this pipeline context:\n\n"
                       + json.dumps(ctx.as_payload(), indent=1, default=str),
        }],
        output_config={"format": {"type": "json_schema", "schema": INSIGHT_SCHEMA}},
    )
    if getattr(response, "stop_reason", None) == "max_tokens":
        raise RuntimeError("Insight truncated at max_tokens; raise max_tokens or trim the context")
    text = "".join(b.text for b in response.content if getattr(b, "type", None) == "text")
    insight = json.loads(text)
    return validate_insight(insight, ctx, model)


def validate_insight(insight: dict[str, Any], ctx: InsightContext, model: str | None = None) -> dict[str, Any]:
    """Drop findings that cite anomalies the pipeline never flagged; enforce severity floor."""
    allowed = ctx.anomaly_keys | {"data_quality"}
    kept = [f for f in insight.get("findings", []) if f.get("anomaly_key") in allowed]
    dropped = len(insight.get("findings", [])) - len(kept)
    insight["findings"] = kept

    # A failed/errored dbt test is never "info", whatever the model said.
    if any(t["status"] in ("fail", "error") for t in ctx.test_issues) and insight.get("severity") == "info":
        insight["severity"] = "warning"
    insight["_meta"] = {"model": model, "dropped_findings": dropped}
    return insight


# ---------------------------------------------------------------- outputs
def insight_record(insight: dict[str, Any], ctx: InsightContext) -> dict[str, Any]:
    """Row for NYC311.AI.DAILY_INSIGHTS."""
    meta = insight.get("_meta", {})
    return {
        "as_of_date": ctx.as_of_date,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
        "model": meta.get("model"),
        "severity": insight["severity"],
        "headline": insight["headline"],
        "summary": insight["summary"],
        "insight": json.dumps({k: v for k, v in insight.items() if k != "_meta"}),
        "context": json.dumps(ctx.as_payload(), default=str),
        "context_hash": ctx.fingerprint(),
        "anomaly_count": len(ctx.anomalies),
        "test_issue_count": len(ctx.test_issues),
        "dropped_findings": meta.get("dropped_findings", 0),
    }


SEVERITY_EMOJI = {"info": ":large_green_circle:", "warning": ":large_yellow_circle:", "critical": ":red_circle:"}


def slack_message(insight: dict[str, Any], ctx: InsightContext, dashboard_url: str | None = None) -> dict[str, Any]:
    lines = [f"{SEVERITY_EMOJI[insight['severity']]} *City 311 data report, {ctx.as_of_date}*",
             f"*{insight['headline']}*", insight["summary"]]
    for f in insight["findings"]:
        lines.append(
            f"• *{f['title']}* ({f['city']}, {f['likely_cause'].replace('_', ' ')}, {f['confidence']} confidence)\n"
            f"  {f['explanation']}\n  _Next:_ {f['recommended_action']}"
        )
    for note in insight.get("data_quality_notes", []):
        lines.append(f"• :mag: {note}")
    if dashboard_url:
        lines.append(f"<{dashboard_url}|Open the dashboard>")
    return {"text": "\n".join(lines)}


def post_to_slack(message: dict[str, Any], webhook_url: str | None = None) -> bool:
    """Post via an incoming webhook. Returns False (and does nothing) when no webhook is configured."""
    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return False
    resp = requests.post(url, json=message, timeout=30)
    resp.raise_for_status()
    return True
