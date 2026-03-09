-- snowflake/databases/DEV/control_views.sql
-- ─────────────────────────────────────────────────────────
-- Convenience views over the control tables.
-- These power the dashboard and give quick visibility
-- into migration status without complex JOINs every time.
-- ─────────────────────────────────────────────────────────

USE ROLE SYSADMIN;
USE DATABASE MIGRATION_ACCELERATOR_DEV;
USE SCHEMA CONTROL;

-- Current status of all tables in the latest run
CREATE OR REPLACE VIEW V_MIGRATION_STATUS AS
SELECT
    R.RUN_NAME,
    R.STATUS                        AS RUN_STATUS,
    T.SOURCE_SCHEMA,
    T.SOURCE_TABLE,
    T.TARGET_SCHEMA,
    T.TARGET_TABLE,
    T.STATUS                        AS TABLE_STATUS,
    T.ROW_COUNT_EST,
    T.UPDATED_AT
FROM MIGRATION_TABLE_REGISTRY T
LEFT JOIN MIGRATION_RUN R ON T.RUN_ID = R.RUN_ID
ORDER BY T.PRIORITY, T.SOURCE_TABLE;

-- Summary counts by status — for dashboard KPI cards
CREATE OR REPLACE VIEW V_MIGRATION_SUMMARY AS
SELECT
    STATUS,
    COUNT(*)                        AS TABLE_COUNT,
    SUM(ROW_COUNT_EST)              AS TOTAL_ROWS_EST
FROM MIGRATION_TABLE_REGISTRY
GROUP BY STATUS;

-- Latest log entries — for real-time activity feed
CREATE OR REPLACE VIEW V_RECENT_ACTIVITY AS
SELECT
    L.PHASE,
    L.STEP_NAME,
    L.STATUS,
    L.ROWS_PROCESSED,
    L.DURATION_SECS,
    L.ERROR_MESSAGE,
    L.LOGGED_AT
FROM PIPELINE_RUN_LOG L
ORDER BY L.LOGGED_AT DESC
LIMIT 50;

-- Validation results summary — green/red per table
CREATE OR REPLACE VIEW V_VALIDATION_SUMMARY AS
SELECT
    T.SOURCE_TABLE,
    V.CHECK_NAME,
    V.STATUS,
    V.SOURCE_VALUE,
    V.TARGET_VALUE,
    V.VARIANCE_PCT,
    V.CHECKED_AT
FROM VALIDATION_RESULTS V
LEFT JOIN MIGRATION_TABLE_REGISTRY T ON V.REGISTRY_ID = T.REGISTRY_ID
ORDER BY T.SOURCE_TABLE, V.CHECK_NAME;