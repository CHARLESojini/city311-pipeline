# Setup and first-run screenshots

Captured during the first end-to-end run (2026-11-15), in order.

| # | File | What it shows |
|---|---|---|
| 1 | `01-snowflake-script-08-run-grants.jpg` | Script 08 run in Snowsight: `REPORTER` role with its grants |
| 2 | `02-snowflake-streamlit-svc-user.jpg` | `STREAMLIT_SVC` service user created (key-pair auth) |
| 3 | `03-airflow-dags-loaded.jpg` | Both DAGs loaded in Airflow 3 with no import errors |
| 4 | `04-airflow-dags-unpaused.jpg` | DAGs unpaused; `city311_transform` waits on the `city311_raw` asset |
| 5 | `05-airflow-ingest-first-run.jpg` | First `city311_ingest` run: three sources extracting in parallel |
| 6 | `06-airflow-aws-profile-error.jpg` | First-run issue: `ProfileNotFound` on the S3 write (fixed in PR #12) |
| 7 | `07-snowflake-stage-file-format-fix.jpg` | Stage pointed at the fully qualified file format (`NYC311.RAW.JSON_FMT`) |
| 8 | `08-airflow-ingest-run-success.jpg` | `city311_ingest` green: all extracts and loads, `city311_raw` asset published |
| 9 | `09-airflow-transform-run-success.jpg` | `city311_transform` triggered by the asset: freshness, dbt, AI report, CSV export |
| 10 | `10-dbt-build-55-pass.jpg` | `dbt build` on real data: PASS=55 WARN=1 ERROR=0 |
| 11 | `11-snowflake-gold-fact-table.jpg` | Gold fact table in Snowflake: NYC and Boston requests with closures |
| 12 | `12-airflow-backfill-60-days.jpg` | 60-day backfill of `city311_ingest` (3 runs at a time) |
| 13 | `13-airflow-ai-report-truncated.jpg` | First AI run: 19 anomalies, Claude's report hit the token cap (fixed in PR #13) |
| 14 | `14-snowflake-ai-report-row.jpg` | Report stored in `AI.DAILY_INSIGHTS` (model, severity, 15 anomalies, 0 dropped findings) |
| 15 | `15-snowflake-ai-report-findings.jpg` | Claude's findings: partial latest-day load, Boston CRM migration, NYC spikes, dbt warning |
| 16 | `16-snowflake-ai-report-after-provisional-fix.jpg` | After PR #14: flagged anomalies drop from 15 to 3 (partial latest day no longer flagged) |
| 17 | `17-snowflake-ai-report-final.jpg` | After PR #15: severity warning (was critical), as of the last complete day; findings are real signals |
| 18 | `18-dashboard-ai-report-and-kpis.jpg` | Live dashboard (city311-pipeline.streamlit.app): AI report and KPI tiles from Snowflake |
| 19 | `19-dashboard-trends-and-migration.jpg` | Requests per day by city, and Boston's legacy-to-new CRM migration |
| 20 | `20-dashboard-hours-to-close.jpg` | Median hours to close, NYC vs Boston, shared categories |
| 21 | `21-dashboard-anomalies-and-backlog.jpg` | Flagged anomalies and open backlog |
| 22 | `22-dashboard-csv-downloads.jpg` | Gold-table CSV downloads (the request-level file ships with the daily export instead) |
| 23 | `23-dashboard-ai-findings-expanded.jpg` | Live dashboard: Claude's AI findings expanded (cause, confidence, next action per finding) |
| 24 | `24-tableau-public-dashboard.jpg` | Tableau Public version built from the gold-table CSVs: daily volume, anomalies, hours to close, open backlog |
