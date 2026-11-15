-- Phase 1 (multi-city): one RAW table per source, each stored exactly as received.
-- Idempotent: safe to re-run.

USE ROLE SYSADMIN;
USE WAREHOUSE NYC311_WH;
USE SCHEMA NYC311.RAW;

-- The original NYC table becomes NYC_SERVICE_REQUESTS. Existing rows and the
-- PIPELINE_LOADER grants move with it. No-op once renamed.
ALTER TABLE IF EXISTS SERVICE_REQUESTS RENAME TO NYC_SERVICE_REQUESTS;

CREATE TABLE IF NOT EXISTS NYC_SERVICE_REQUESTS (
  raw_data     VARIANT,
  source_file  STRING,
  loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Boston legacy CRM (12-digit case_enquiry_id), winding down during the migration
CREATE TABLE IF NOT EXISTS BOSTON_LEGACY_SERVICE_REQUESTS (
  raw_data     VARIANT,
  source_file  STRING,
  loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Boston new CRM (BCS- case_id), live since October 2025
CREATE TABLE IF NOT EXISTS BOSTON_NEW_SERVICE_REQUESTS (
  raw_data     VARIANT,
  source_file  STRING,
  loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

GRANT SELECT, INSERT ON TABLE NYC_SERVICE_REQUESTS           TO ROLE PIPELINE_LOADER;
GRANT SELECT, INSERT ON TABLE BOSTON_LEGACY_SERVICE_REQUESTS TO ROLE PIPELINE_LOADER;
GRANT SELECT, INSERT ON TABLE BOSTON_NEW_SERVICE_REQUESTS    TO ROLE PIPELINE_LOADER;

-- New landing layout: s3://<bucket>/landing/<source>/run_date=YYYY-MM-DD/<source>_YYYYMMDD.json
-- Files from the first NYC loads (landing/nyc/created_date=...) are already in
-- NYC_SERVICE_REQUESTS and stay valid.
SELECT 'NYC' AS src, COUNT(*) FROM NYC_SERVICE_REQUESTS
UNION ALL SELECT 'BOSTON_LEGACY', COUNT(*) FROM BOSTON_LEGACY_SERVICE_REQUESTS
UNION ALL SELECT 'BOSTON_NEW', COUNT(*) FROM BOSTON_NEW_SERVICE_REQUESTS;
