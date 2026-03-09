-- snowflake/config/resource_monitor.sql
-- ─────────────────────────────────────────────────────────
-- A Resource Monitor caps your Snowflake spend.
-- If credit usage hits the limit, all warehouses suspend.
-- This is your safety net against accidental runaway costs.
-- ─────────────────────────────────────────────────────────

-- Must be run as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR MIGRATION_COST_GUARD
    WITH 
        CREDIT_QUOTA = 5           -- Stop everything at 5 credits (~$15 at on-demand pricing)
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS 
            ON 75 PERCENT DO NOTIFY          -- Email warning at 75%
            ON 100 PERCENT DO SUSPEND_IMMEDIATE;  -- Hard stop at 100%

-- Apply the monitor to all three warehouses
ALTER WAREHOUSE MIGRATION_WH  SET RESOURCE_MONITOR = MIGRATION_COST_GUARD;
ALTER WAREHOUSE VALIDATION_WH SET RESOURCE_MONITOR = MIGRATION_COST_GUARD;
ALTER WAREHOUSE APP_WH        SET RESOURCE_MONITOR = MIGRATION_COST_GUARD;

-- Switch back to normal role
USE ROLE SYSADMIN;