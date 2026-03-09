-- snowflake/databases/DEV/control_layer.sql
-- ─────────────────────────────────────────────────────────
-- The CONTROL schema is the brain of the accelerator.
-- All tables here are designed for extensibility —
-- new behaviors are added via INSERT, never ALTER.
-- ─────────────────────────────────────────────────────────

USE SYSADMIN;

USE DATABASE MIGRATION_ACCELERATOR_DEV;
USE SCHEMA CONTROL;

USE DATABASE MIGRATION_ACCELERATOR_DEV;
USE SCHEMA CONTROL;

-- Ensure SYSADMIN has full control over all objects in this schema
GRANT ALL PRIVILEGES ON SCHEMA MIGRATION_ACCELERATOR_DEV.CONTROL TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA MIGRATION_ACCELERATOR_DEV.CONTROL TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA MIGRATION_ACCELERATOR_DEV.CONTROL TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA MIGRATION_ACCELERATOR_DEV.CONTROL TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA MIGRATION_ACCELERATOR_DEV.CONTROL TO ROLE SYSADMIN;

-- ── 1. MIGRATION_RUN ──────────────────────────────────────
-- Every time the pipeline runs, it creates one row here.
-- Think of it as the parent record for everything else.
CREATE TABLE IF NOT EXISTS MIGRATION_RUN (
    RUN_ID          VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    RUN_NAME        VARCHAR(255),
    SOURCE_TYPE     VARCHAR(50)     DEFAULT 'REDSHIFT',
    TARGET_TYPE     VARCHAR(50)     DEFAULT 'SNOWFLAKE',
    STATUS          VARCHAR(20)     DEFAULT 'STARTED',
    TRIGGERED_BY    VARCHAR(100),
    STARTED_AT      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT    TIMESTAMP_NTZ,
    NOTES           VARCHAR(2000),
    CONSTRAINT PK_MIGRATION_RUN PRIMARY KEY (RUN_ID)
);

-- ── 2. MIGRATION_TABLE_REGISTRY ───────────────────────────
-- Every table to be migrated is registered here.
-- STATUS drives the pipeline — the orchestrator reads
-- PENDING tables and processes them in order.
-- OCP: new source types added as new rows, not new columns.
CREATE TABLE IF NOT EXISTS MIGRATION_TABLE_REGISTRY (
    REGISTRY_ID     VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    RUN_ID          VARCHAR(36),
    SOURCE_SCHEMA   VARCHAR(255)    NOT NULL,
    SOURCE_TABLE    VARCHAR(255)    NOT NULL,
    TARGET_SCHEMA   VARCHAR(255),
    TARGET_TABLE    VARCHAR(255),
    ROW_COUNT_EST   NUMBER,
    SIZE_BYTES_EST  NUMBER,
    PRIORITY        NUMBER          DEFAULT 100,
    STATUS          VARCHAR(20)     DEFAULT 'PENDING',
    -- PENDING → IN_PROGRESS → LOADED → VALIDATED → COMPLETE / FAILED
    REGISTERED_AT   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_TABLE_REGISTRY PRIMARY KEY (REGISTRY_ID)
);

-- ── 3. PIPELINE_RUN_LOG ───────────────────────────────────
-- Detailed step-by-step log for every action taken.
-- Every stored procedure writes here on start AND finish.
-- This is your audit trail and debugging tool.
CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG (
    LOG_ID          VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    RUN_ID          VARCHAR(36),
    REGISTRY_ID     VARCHAR(36),
    PHASE           VARCHAR(50),
    -- ASSESSMENT | TRANSLATION | LOAD | VALIDATION
    STEP_NAME       VARCHAR(255),
    STATUS          VARCHAR(20),
    -- STARTED | SUCCESS | FAILED | SKIPPED
    ROWS_PROCESSED  NUMBER,
    DURATION_SECS   NUMBER,
    ERROR_MESSAGE   VARCHAR(5000),
    LOGGED_AT       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_RUN_LOG PRIMARY KEY (LOG_ID)
);

-- ── 4. TRANSLATION_CONFIG ─────────────────────────────────
-- Rules for translating Redshift objects to Snowflake.
-- OCP in action: new translation rules = new rows here.
-- The translation procedure reads this table at runtime.
CREATE TABLE IF NOT EXISTS TRANSLATION_CONFIG (
    CONFIG_ID       VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    CONFIG_TYPE     VARCHAR(50)     NOT NULL,
    -- DATA_TYPE_MAP | FUNCTION_MAP | CLAUSE_STRIP | PATTERN_REPLACE
    SOURCE_VALUE    VARCHAR(500)    NOT NULL,
    TARGET_VALUE    VARCHAR(500),
    IS_REGEX        BOOLEAN         DEFAULT FALSE,
    PRIORITY        NUMBER          DEFAULT 100,
    IS_ACTIVE       BOOLEAN         DEFAULT TRUE,
    NOTES           VARCHAR(1000),
    CONSTRAINT PK_TRANSLATION_CONFIG PRIMARY KEY (CONFIG_ID)
);

-- ── 5. VALIDATION_CONFIG ──────────────────────────────────
-- Defines which validation checks run per table.
-- OCP: new check types registered here, not hardcoded.
CREATE TABLE IF NOT EXISTS VALIDATION_CONFIG (
    CONFIG_ID       VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    CHECK_NAME      VARCHAR(100)    NOT NULL,
    CHECK_TYPE      VARCHAR(50)     NOT NULL,
    -- ROW_COUNT | NULL_RATE | CHECKSUM | CUSTOM
    IS_ACTIVE       BOOLEAN         DEFAULT TRUE,
    FAIL_THRESHOLD  NUMBER          DEFAULT 0.01,
    -- Acceptable variance (1% default)
    NOTES           VARCHAR(1000),
    CONSTRAINT PK_VALIDATION_CONFIG PRIMARY KEY (CONFIG_ID)
);

-- ── 6. VALIDATION_RESULTS ─────────────────────────────────
-- Stores the outcome of every validation check per table.
-- Powers the dashboard — green/red status per table.
CREATE TABLE IF NOT EXISTS VALIDATION_RESULTS (
    RESULT_ID           VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    RUN_ID              VARCHAR(36),
    REGISTRY_ID         VARCHAR(36),
    CHECK_NAME          VARCHAR(100),
    SOURCE_VALUE        VARIANT,
    TARGET_VALUE        VARIANT,
    VARIANCE_PCT        NUMBER,
    STATUS              VARCHAR(20),
    -- PASS | FAIL | WARNING
    CHECKED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_VALIDATION_RESULTS PRIMARY KEY (RESULT_ID)
);