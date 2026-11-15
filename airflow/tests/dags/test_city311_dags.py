"""Structure tests for the city311 DAGs. Needs Airflow (astro dev pytest, or CI)."""
from __future__ import annotations

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_ingest_runs_every_source_in_parallel(dagbag):
    dag = dagbag.get_dag("city311_ingest")
    for source in ("nyc", "boston_legacy", "boston_new"):
        extract = dag.get_task(f"extract_{source}")
        assert extract.downstream_task_ids == {f"load_{source}"}
        assert dag.get_task(f"load_{source}").downstream_task_ids == {"publish_raw"}
        assert extract.upstream_task_ids == set()  # sources do not wait on each other


def test_ingest_publishes_raw_asset(dagbag):
    outlets = dagbag.get_dag("city311_ingest").get_task("publish_raw").outlets
    assert [a.name for a in outlets] == ["city311_raw"]


def test_transform_is_triggered_by_raw_asset(dagbag):
    dag = dagbag.get_dag("city311_transform")
    assert "city311_raw" in str(dag.timetable.asset_condition)
    assert dag.get_task("source_freshness").downstream_task_ids == {"dbt_build"}


def test_dbt_build_does_not_retry(dagbag):
    # a failing data test fails again on retry; alert immediately instead
    assert dagbag.get_dag("city311_transform").get_task("dbt_build").retries == 0


def test_ai_insights_runs_even_if_dbt_fails(dagbag):
    task = dagbag.get_dag("city311_transform").get_task("ai_insights")
    assert task.upstream_task_ids == {"dbt_build"}
    assert task.trigger_rule == "all_done"


def test_csv_export_follows_successful_gold_build(dagbag):
    task = dagbag.get_dag("city311_transform").get_task("export_gold_csv")
    assert task.upstream_task_ids == {"publish_gold"}
    assert task.trigger_rule == "all_success"
