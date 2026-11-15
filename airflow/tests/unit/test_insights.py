"""Unit tests for the AI insights layer. A fake client stands in for Claude."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from include.city311 import insights as ins


def ctx(anomalies=None, tests=None) -> ins.InsightContext:
    return ins.InsightContext(
        as_of_date="2026-11-13",
        anomalies=anomalies or [],
        city_kpis=[{"city": "NYC", "requests_last_7d": 1173}],
        recent_volume=[],
        test_issues=tests or [],
    )


ANOMALY = {
    "anomaly_key": "category:NYC:Noise:2026-11-12", "activity_date": "2026-11-12",
    "series_type": "category", "city": "NYC", "series_value": "Noise", "direction": "spike",
    "request_count": 168, "baseline_mean": 28.5, "z_score": 21.4, "pct_change_vs_baseline": 4.9,
}


def finding(key: str) -> dict:
    return {
        "anomaly_key": key, "city": "NYC", "title": "Noise surge", "explanation": "x",
        "likely_cause": "real_world_event", "confidence": "medium", "recommended_action": "y",
    }


class FakeClient:
    def __init__(self, payload: dict, stop_reason: str = "end_turn"):
        self.payload, self.stop_reason, self.calls = payload, stop_reason, []
        self.messages = self

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            stop_reason=self.stop_reason,
            content=[SimpleNamespace(type="thinking", thinking="..."),
                     SimpleNamespace(type="text", text=json.dumps(self.payload))],
        )


def report(findings, severity="warning"):
    return {"headline": "h", "severity": severity, "summary": "s", "findings": findings, "data_quality_notes": []}


def test_no_signal_skips_the_model_entirely():
    out = ins.generate_insight(ctx(), client=None)
    assert out["severity"] == "info" and out["findings"] == []
    assert out["_meta"]["model"] is None


def test_signal_without_client_raises():
    with pytest.raises(RuntimeError):
        ins.generate_insight(ctx([ANOMALY]), client=None)


def test_request_uses_structured_output_and_only_computed_facts():
    client = FakeClient(report([finding(ANOMALY["anomaly_key"])]))
    out = ins.generate_insight(ctx([ANOMALY]), client, model="claude-haiku-5-5")
    call = client.calls[0]
    assert call["output_config"]["format"] == {"type": "json_schema", "schema": ins.INSIGHT_SCHEMA}
    assert "temperature" not in call  # newer models reject non-default sampling params
    assert "tool_choice" not in call  # forced tool use is not supported on newer models
    assert ANOMALY["anomaly_key"] in call["messages"][0]["content"]
    assert len(out["findings"]) == 1 and out["_meta"]["dropped_findings"] == 0


def test_hallucinated_findings_are_dropped():
    client = FakeClient(report([finding(ANOMALY["anomaly_key"]), finding("city:Boston:Boston:2026-11-12")]))
    out = ins.generate_insight(ctx([ANOMALY]), client)
    assert [f["anomaly_key"] for f in out["findings"]] == [ANOMALY["anomaly_key"]]
    assert out["_meta"]["dropped_findings"] == 1


def test_failed_dbt_test_forces_at_least_warning():
    tests = [{"test": "unique_fct_service_requests_request_key", "status": "fail", "failing_rows": 3, "message": ""}]
    client = FakeClient(report([finding("data_quality")], severity="info"))
    out = ins.generate_insight(ctx(tests=tests), client)
    assert out["severity"] == "warning"
    assert out["findings"][0]["anomaly_key"] == "data_quality"


def test_truncated_response_raises():
    with pytest.raises(RuntimeError, match="max_tokens"):
        ins.generate_insight(ctx([ANOMALY]), FakeClient(report([]), stop_reason="max_tokens"))


def test_schema_is_strict_everywhere():
    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "object":
                assert node.get("additionalProperties") is False
                assert set(node["required"]) == set(node["properties"])
            for v in node.values():
                walk(v)
    walk(ins.INSIGHT_SCHEMA)


def test_read_dbt_artifacts(tmp_path):
    (tmp_path / "run_results.json").write_text(json.dumps({"results": [
        {"unique_id": "test.city311.unique_fct_service_requests_request_key.a1b2", "status": "fail", "failures": 3},
        {"unique_id": "test.city311.assert_invalid_closure_rate_is_low", "status": "warn", "failures": 1},
        {"unique_id": "model.city311.fct_service_requests", "status": "success"},
    ]}))
    (tmp_path / "sources.json").write_text(json.dumps({"results": [
        {"unique_id": "source.city311.raw.boston_new_service_requests", "status": "warn",
         "max_loaded_at_time_ago_in_s": 151200},
        {"unique_id": "source.city311.raw.nyc_service_requests", "status": "pass"},
    ]}))
    tests = ins.read_dbt_test_issues(tmp_path / "run_results.json")
    assert [t["test"] for t in tests] == [
        "unique_fct_service_requests_request_key", "assert_invalid_closure_rate_is_low"]
    stale = ins.read_stale_sources(tmp_path / "sources.json")
    assert stale == [{"source": "boston_new_service_requests", "status": "warn", "hours_since_load": 42.0}]
    assert ins.read_dbt_test_issues(tmp_path / "missing.json") == []


def test_record_and_slack_message():
    c = ctx([ANOMALY])
    insight = ins.validate_insight(report([finding(ANOMALY["anomaly_key"])]), c, "claude-haiku-5-5")
    row = ins.insight_record(insight, c)
    assert row["anomaly_count"] == 1 and row["model"] == "claude-haiku-5-5"
    assert "_meta" not in json.loads(row["insight"])
    assert len(row["context_hash"]) == 16
    text = ins.slack_message(insight, c, "https://example.streamlit.app")["text"]
    assert "Noise surge" in text and "real world event" in text and "example.streamlit.app" in text


def test_slack_is_skipped_without_webhook(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    assert ins.post_to_slack({"text": "x"}) is False


def test_load_context_against_duckdb_gold(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(str(tmp_path / "g.duckdb"))
    con.execute("create schema gold")
    con.execute("""create table gold.mart_volume_anomalies as select * from (values
        ('category:NYC:Noise:2026-11-12', date '2026-11-12', 'category', 'NYC', 'Noise', 'spike', 168, 28.5, 21.4, 4.9, true),
        ('city:NYC:NYC:2026-11-13',       date '2026-11-13', 'city', 'NYC', 'NYC', 'spike', 160, 150.0, 0.5, 0.06, false),
        ('city:NYC:NYC:2026-10-01',       date '2026-10-01', 'city', 'NYC', 'NYC', 'drop', 10, 150.0, -9.0, -0.9, true)
      ) t(anomaly_key, activity_date, series_type, city, series_value, direction, request_count,
          baseline_mean, z_score, pct_change_vs_baseline, is_anomaly)""")
    con.execute("create table gold.mart_city_kpis as select 'NYC' as city, 1173 as requests_last_7d")
    con.execute("""create table gold.mart_daily_city_volume as
        select date '2026-11-13' as activity_date, 'NYC' as city, 'nyc' as source_system, 160 as opened_count""")

    def query(sql):
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    c = ins.load_context(query, "gold")
    assert c.as_of_date == "2026-11-13"
    # only flagged anomalies from the recent window; the October one is history
    assert [a["anomaly_key"] for a in c.anomalies] == ["category:NYC:Noise:2026-11-12"]
    assert c.recent_volume[0]["opened"] == 160
