-- snowflake/warehouses/init.sql
-- ─────────────────────────────────────────────────────────
-- Creates all warehouses for the accelerator.
-- AUTO_SUSPEND = 60 means the warehouse shuts off after
-- 60 seconds of inactivity — critical for cost control.
-- ─────────────────────────────────────────────────────────

-- Main warehouse for migration workloads (loading, translation)
CREATE WAREHOUSE IF NOT EXISTS MIGRATION_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Primary warehouse for migration pipeline workloads';

-- Separate warehouse for validation queries (keeps them from blocking loads)
CREATE WAREHOUSE IF NOT EXISTS VALIDATION_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Validation and QA queries';

-- Tiny warehouse for the dashboard app
CREATE WAREHOUSE IF NOT EXISTS APP_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Streamlit dashboard queries';