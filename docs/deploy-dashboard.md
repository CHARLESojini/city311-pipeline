# Deploying the dashboard

Two front ends read the same gold layer:

| | Streamlit (live) | Tableau Public (snapshot) |
|---|---|---|
| Data | Queries Snowflake GOLD directly, cached 1 hour | CSV extracts of GOLD |
| Refresh | Automatic after each daily dbt run | Re-upload the CSVs (or re-run the export) |
| Hosting | Streamlit Community Cloud (free) | Tableau Public (free) |
| Credentials | Read-only `STREAMLIT_SVC` user, role `REPORTER` | None: public CSVs only |

## 1. Streamlit Community Cloud

### One-time Snowflake setup

```bash
# A separate key pair for the dashboard, so it can be revoked on its own
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.snowflake/rsa_key_streamlit.p8 -nocrypt
openssl rsa -in ~/.snowflake/rsa_key_streamlit.p8 -pubout -out ~/.snowflake/rsa_key_streamlit.pub
```

1. Run `snowflake/08_reporting_access.sql`, pasting the **public** key body
   (without the BEGIN/END lines) where it says `<STREAMLIT_RSA_PUBLIC_KEY>`.
2. Run dbt once in prod (`dbt build --target prod`, or let the DAG run). Its
   `grant_gold_access` hook grants `REPORTER` read access to `GOLD`.

### Deploy

1. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub.
2. **Create app**, then **Deploy a public app from GitHub**:
   - Repository: `CHARLESojini/city311-pipeline`
   - Branch: `main`
   - Main file path: `dashboard/app.py`
   - Advanced settings: Python 3.12
3. Under **Advanced settings > Secrets**, paste the contents of
   `dashboard/.streamlit/secrets.toml.example` with real values: your account
   identifier and the full PEM text of `rsa_key_streamlit.p8`.
4. Deploy. The caption under the title should read **Live: Snowflake**.
   If it says *Demo data*, the `[snowflake]` secrets section was not found.

Streamlit Cloud installs `dashboard/requirements.txt` because it sits next to `app.py`.

### Run it locally

```bash
pip install -r dashboard/requirements.txt
cp dashboard/.streamlit/secrets.toml.example dashboard/.streamlit/secrets.toml   # gitignored; fill in
streamlit run dashboard/app.py
```

Delete the `[snowflake]` section, or skip creating `secrets.toml`, to run on the
DuckDB demo warehouse instead (build it first, see the README's *Run it without Snowflake*).

### Cost guard rails

The dashboard caches every query for an hour and uses the XS warehouse with
60-second auto-suspend. `08_reporting_access.sql` includes an optional resource
monitor to hard-cap credits on the trial account.

## 2. Tableau Public

Tableau Public cannot connect to Snowflake, so it reads CSV extracts of GOLD.

### Get the CSVs

Use any of these:
- **Daily, automatic:** the transform DAG writes them to
  `s3://$CITY311_BUCKET/exports/tableau/latest/` (plus a dated copy).
  `aws s3 sync s3://$CITY311_BUCKET/exports/tableau/latest/ exports/tableau/`
- **On demand:** `python scripts/export_gold_csv.py --source snowflake`
- **From the live app:** the *Download the gold tables* buttons at the bottom.

| File | Grain | Good for |
|---|---|---|
| `daily_city_volume.csv` | day × city × source system × category | trend lines, migration view |
| `category_performance.csv` | city × category (last 28 days) | NYC vs Boston resolution times |
| `open_backlog.csv` | city × category | backlog bars |
| `city_kpis.csv` | city | KPI tiles |
| `volume_anomalies.csv` | flagged day × series | anomaly timeline |
| `service_requests_recent.csv` | one row per request (last 90 days) | maps (lat/long), drill-down |
| `ai_daily_insights.csv` | one row per daily AI report | annotation / text panel |

### Build the workbook

1. Open Tableau Public (desktop app) > **Connect > Text file** > `daily_city_volume.csv`.
   Add the other CSVs as separate data sources; they do not need to be joined.
2. Suggested sheets, mirroring the Streamlit app:
   - **Requests per day:** `activity_date` (continuous day) on Columns,
     SUM(`opened_count`) on Rows, `city` on Rows as well (one panel per city,
     independent axes), `city` on Color.
   - **Boston migration:** filter `city` = Boston, `source_system` on Color.
   - **Hours to close:** `standard_category` on Rows, `median_resolution_hours`
     on Columns, `city` on Color. Exclude `Other`.
   - **Map:** `service_requests_recent.csv`, latitude/longitude, `standard_category` on Color.
3. Colors, to match the app: NYC `#2a78d6`, Boston `#eb6834`,
   legacy CRM `#1baf7a`, new CRM `#eda100`.
4. **File > Save to Tableau Public.** To refresh, replace the CSVs and re-save.
