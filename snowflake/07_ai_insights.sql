-- Phase 4: where the AI layer writes its daily reports. Idempotent.
-- Read access to GOLD for PIPELINE_LOADER is granted by dbt itself after each
-- prod run (macro grant_gold_access), because dbt (TRANSFORMER) owns GOLD.
USE ROLE SYSADMIN;
USE WAREHOUSE NYC311_WH;

CREATE SCHEMA IF NOT EXISTS NYC311.AI;

CREATE TABLE IF NOT EXISTS NYC311.AI.DAILY_INSIGHTS (
  as_of_date        DATE,
  generated_at      TIMESTAMP_NTZ,
  model             STRING,          -- NULL when nothing was flagged and no model was called
  severity          STRING,          -- info | warning | critical
  headline          STRING,
  summary           STRING,
  insight           VARIANT,         -- full structured report (findings, notes)
  context           VARIANT,         -- exactly what the model was shown
  context_hash      STRING,
  anomaly_count     INTEGER,
  test_issue_count  INTEGER,
  dropped_findings  INTEGER          -- findings rejected for citing undetected anomalies
);

GRANT USAGE ON SCHEMA NYC311.AI TO ROLE PIPELINE_LOADER;
GRANT SELECT, INSERT ON TABLE NYC311.AI.DAILY_INSIGHTS TO ROLE PIPELINE_LOADER;
