-- Phase 5: read-only access for the public Streamlit dashboard. Idempotent.
-- The app can read GOLD and the AI reports and nothing else: no RAW, no writes.
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS REPORTER;
GRANT ROLE REPORTER TO ROLE SYSADMIN;

USE ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE NYC311_WH TO ROLE REPORTER;
GRANT USAGE ON DATABASE NYC311 TO ROLE REPORTER;
-- GOLD usage + SELECT are granted by dbt after each prod run (macro grant_gold_access)
GRANT USAGE ON SCHEMA NYC311.AI TO ROLE REPORTER;
GRANT SELECT ON TABLE NYC311.AI.DAILY_INSIGHTS TO ROLE REPORTER;

-- A dedicated key-pair service user, so the dashboard's credentials can be
-- rotated or revoked without touching the pipeline's.
USE ROLE SECURITYADMIN;
CREATE USER IF NOT EXISTS STREAMLIT_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = REPORTER
  DEFAULT_WAREHOUSE = NYC311_WH;
-- Generate a separate key pair for this user (see README); paste the PUBLIC key only.
ALTER USER STREAMLIT_SVC SET RSA_PUBLIC_KEY = '<STREAMLIT_RSA_PUBLIC_KEY>';
GRANT ROLE REPORTER TO USER STREAMLIT_SVC;

-- Optional guard rail: cap what a public app can spend on the trial account
-- USE ROLE ACCOUNTADMIN;
-- CREATE RESOURCE MONITOR IF NOT EXISTS DASHBOARD_MONITOR WITH CREDIT_QUOTA = 5
--   TRIGGERS ON 90 PERCENT DO SUSPEND;
-- ALTER WAREHOUSE NYC311_WH SET RESOURCE_MONITOR = DASHBOARD_MONITOR;
