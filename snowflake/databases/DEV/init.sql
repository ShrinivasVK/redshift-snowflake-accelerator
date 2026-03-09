-- snowflake/databases/DEV/init.sql
-- ─────────────────────────────────────────────────────────
-- Creates the DEV environment foundation.
-- This runs ONCE when setting up a new environment.
-- Uses CREATE OR REPLACE so it's safe to re-run (idempotent).
-- ─────────────────────────────────────────────────────────

-- DEV Database
CREATE DATABASE IF NOT EXISTS MIGRATION_ACCELERATOR_DEV
    COMMENT = 'ArisData Migration Accelerator — Development Environment';

-- Schemas (one per functional layer — Open/Closed principle)
CREATE SCHEMA IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.CONTROL
    COMMENT = 'Orchestration: queues, run logs, config tables';

CREATE SCHEMA IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.ASSESSMENT
    COMMENT = 'Source environment profiling outputs';

CREATE SCHEMA IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.TRANSLATION
    COMMENT = 'DDL and SQL translation outputs';

CREATE SCHEMA IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.STAGING
    COMMENT = 'Intermediate tables during data loading';

CREATE SCHEMA IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.TARGET
    COMMENT = 'Final migrated tables';

CREATE SCHEMA IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.VALIDATION
    COMMENT = 'Post-migration quality check results';

CREATE SCHEMA IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.APP
    COMMENT = 'Streamlit dashboard objects';