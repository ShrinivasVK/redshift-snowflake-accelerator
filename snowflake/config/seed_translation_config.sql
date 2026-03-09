-- snowflake/config/seed_translation_config.sql
-- ─────────────────────────────────────────────────────────
-- Seeds the TRANSLATION_CONFIG table with the core
-- Redshift → Snowflake translation rules.
-- Adding new rules = INSERT new rows. Never edit this file
-- to modify existing rules — add a new row and deactivate
-- the old one. This preserves the full audit history.
-- ─────────────────────────────────────────────────────────

USE DATABASE MIGRATION_ACCELERATOR_DEV;
USE SCHEMA CONTROL;

-- Data type mappings
INSERT INTO TRANSLATION_CONFIG
    (CONFIG_TYPE, SOURCE_VALUE, TARGET_VALUE, NOTES)
VALUES
    ('DATA_TYPE_MAP', 'TIMESTAMP',      'TIMESTAMP_NTZ',    'Redshift TIMESTAMP has no TZ'),
    ('DATA_TYPE_MAP', 'TIMESTAMPTZ',    'TIMESTAMP_TZ',     'Timezone-aware timestamp'),
    ('DATA_TYPE_MAP', 'SUPER',          'VARIANT',          'Semi-structured data type'),
    ('DATA_TYPE_MAP', 'HLLSKETCH',      'VARCHAR',          'Approximate distinct count — stub'),
    ('DATA_TYPE_MAP', 'FLOAT4',         'FLOAT4',           'Direct equivalent'),
    ('DATA_TYPE_MAP', 'FLOAT8',         'FLOAT8',           'Direct equivalent'),
    ('DATA_TYPE_MAP', 'BOOL',           'BOOLEAN',          'Direct equivalent');

-- DDL clauses to strip (Redshift-specific, not valid in Snowflake)
INSERT INTO TRANSLATION_CONFIG
    (CONFIG_TYPE, SOURCE_VALUE, TARGET_VALUE, NOTES)
VALUES
    ('CLAUSE_STRIP', 'DISTKEY\\([^)]+\\)',      '',     'Remove distribution key'),
    ('CLAUSE_STRIP', 'SORTKEY\\([^)]+\\)',      '',     'Remove sort key'),
    ('CLAUSE_STRIP', 'DISTSTYLE\\s+\\w+',       '',     'Remove distribution style'),
    ('CLAUSE_STRIP', 'ENCODE\\s+\\w+',          '',     'Remove column encoding'),
    ('CLAUSE_STRIP', 'INTERLEAVED\\s+SORTKEY',  '',     'Remove interleaved sort key');

-- Function mappings
INSERT INTO TRANSLATION_CONFIG
    (CONFIG_TYPE, SOURCE_VALUE, TARGET_VALUE, NOTES)
VALUES
    ('FUNCTION_MAP', 'GETDATE()',   'CURRENT_TIMESTAMP()',  'Current timestamp'),
    ('FUNCTION_MAP', 'ISNULL(',     'NVL(',                 'Null coalescing'),
    ('FUNCTION_MAP', 'TOP ',        'LIMIT ',               'Row limiting — positional replacement');

-- Seed validation checks
INSERT INTO VALIDATION_CONFIG
    (CHECK_NAME, CHECK_TYPE, FAIL_THRESHOLD, NOTES)
VALUES
    ('Row Count Match',     'ROW_COUNT',    0.001,  'Fail if row counts differ by more than 0.1%'),
    ('Null Rate Match',     'NULL_RATE',    0.01,   'Fail if null rates differ by more than 1%'),
    ('Sample Checksum',     'CHECKSUM',     0.0,    'Exact match required on 10% sample');