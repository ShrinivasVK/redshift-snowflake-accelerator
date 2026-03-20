-- ============================================================
--  ArisData — Redshift to Snowflake Migration Accelerator
--  DEMO WALKTHROUGH SCRIPT
--
--  Purpose  : Single executable script for live demo and
--             presentation. Each step contains:
--               (1) A description block explaining the step
--               (2) The execution SQL (procedure call or DML)
--               (3) A verification query to show the result
--
--  Dataset  : TICKIT — 7 tables, ~624K rows total
--             Source : s3://awssampledbuswest2/tickit/
--
--  Run Order: Execute each numbered block in sequence.
--             The session variable $RUN_ID carries through
--             all steps — do NOT reset it mid-session.
-- ============================================================


-- ============================================================
--  STEP 1 — CREATE MIGRATION RUN
-- ============================================================
/*
  WHAT THIS STEP DOES
  -------------------
  Every pipeline execution is anchored to a parent record
  called a "Migration Run". This record acts as the unique
  identifier (RUN_ID) that links every log entry, validation
  result, and table status back to this specific execution.

  Think of it as opening a case file before work begins.
  Nothing in the pipeline writes output without a RUN_ID.

  WHERE DATA COMES FROM
  ----------------------
  No external data source. UUID_STRING() generates a unique
  ID within Snowflake. The run name and trigger are provided
  inline.

  PURPOSE
  -------
  Establish the RUN_ID that all downstream steps will use.
  Without this, no step can log or track its output.

  DATABASE OBJECTS USED
  ----------------------
  - CONTROL.MIGRATION_RUN  (target table for the new record)

  DATABASE OBJECTS AFFECTED
  --------------------------
  - CONTROL.MIGRATION_RUN  → 1 new row inserted with status
                             'IN_PROGRESS'
  - Session variable $RUN_ID is set for use in all steps below
*/

-- EXECUTE — Create the run record
INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN (
    RUN_ID,
    RUN_NAME,
    STATUS,
    TRIGGERED_BY
)
SELECT
    UUID_STRING(),
    'POC Demo Walkthrough Run',
    'IN_PROGRESS',
    'DEMO_SCRIPT';

-- Capture RUN_ID into a session variable
SET RUN_ID = (
    SELECT RUN_ID
    FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN
    ORDER BY STARTED_AT DESC
    LIMIT 1
);

-- VERIFY — Confirm the run record was created
SELECT
    RUN_ID,
    RUN_NAME,
    STATUS,
    TRIGGERED_BY,
    STARTED_AT
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN
WHERE
    RUN_ID = $RUN_ID;

/*
  EXPECTED RESULT
  ---------------
  One row with:
    STATUS       = IN_PROGRESS
    TRIGGERED_BY = DEMO_SCRIPT
    RUN_ID       = a UUID that carries through all steps below
*/


-- ============================================================
--  STEP 2 — ASSESS THE SOURCE SCHEMA
-- ============================================================
/*
  WHAT THIS STEP DOES
  -------------------
  The assessment procedure scans the source schema (TICKIT in
  this demo) and builds a complete inventory of every table
  that needs to be migrated. For each table it records the
  column count, estimated row volume, and sets the initial
  migration status to PENDING.

  This is the "discovery" phase. In a real engagement, this
  would run against the client's actual Redshift environment
  and produce the same structured output.

  WHERE DATA COMES FROM
  ----------------------
  REDSHIFT_MIRROR schema — external tables in Snowflake that
  point to CSV files at s3://awssampledbuswest2/tickit/

  This simulates Redshift UNLOAD output. The external tables
  expose the same structure and row data a real Redshift
  UNLOAD would produce.

  PURPOSE
  -------
  Build the MIGRATION_TABLE_REGISTRY — the master list of
  tables that every downstream step (translation, load,
  validation) reads to know what to process.

  DATABASE OBJECTS USED
  ----------------------
  - REDSHIFT_MIRROR.*          (source: 7 external tables)
  - CONTROL.TRANSLATION_CONFIG (read for schema context)

  DATABASE OBJECTS AFFECTED
  --------------------------
  - CONTROL.MIGRATION_TABLE_REGISTRY → 7 rows inserted,
    one per TICKIT table, all with STATUS = 'PENDING'
  - CONTROL.PIPELINE_RUN_LOG → STARTED and SUCCESS rows
    logged for SP_ASSESS_SCHEMA
*/

-- EXECUTE — Run schema assessment
CALL MIGRATION_ACCELERATOR_DEV.ASSESSMENT.SP_ASSESS_SCHEMA(
    $RUN_ID,
    'tickit'
);

-- VERIFY — Confirm all 7 tables are registered
SELECT
    SOURCE_TABLE,
    ROW_COUNT_EST,
    SIZE_BYTES_EST,
    STATUS
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
WHERE
    RUN_ID = $RUN_ID
ORDER BY
    SOURCE_TABLE;

/*
  EXPECTED RESULT
  ---------------
  7 rows — one for each TICKIT table:
    CATEGORY  |  STATUS = PENDING
    DATE      |  STATUS = PENDING
    EVENT     |  STATUS = PENDING
    LISTING   |  STATUS = PENDING
    SALES     |  STATUS = PENDING
    USERS     |  STATUS = PENDING
    VENUE     |  STATUS = PENDING
*/

-- VERIFY — Confirm assessment was logged in the pipeline log
SELECT
    PHASE,
    STEP_NAME,
    STATUS,
    ROWS_PROCESSED,
    DURATION_SECS,
    LOGGED_AT
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
WHERE
    RUN_ID = $RUN_ID
    AND PHASE = 'ASSESSMENT'
ORDER BY
    LOGGED_AT;


/*
  EXPECTED RESULT
  ---------------
  2 rows:
    STEP_NAME = SP_ASSESS_SCHEMA | STATUS = STARTED
    STEP_NAME = SP_ASSESS_SCHEMA | STATUS = SUCCESS
*/


-- ============================================================
--  STEP 3A — TRANSLATE DDL FROM REDSHIFT TO SNOWFLAKE SYNTAX
-- ============================================================
/*
  WHAT THIS STEP DOES
  -------------------
  Reads the Redshift CREATE TABLE statements for every table
  in the registry and converts them to Snowflake-compatible
  syntax. The translation is rule-based, driven entirely by
  the TRANSLATION_CONFIG table — no hardcoded logic in the
  procedure itself.

  Key transformations applied:
    - DISTKEY, SORTKEY, DISTSTYLE clauses are stripped out
      (Snowflake handles distribution automatically)
    - ENCODE clauses are removed
      (Snowflake uses its own compression internally)
    - TIMESTAMP → TIMESTAMP_NTZ
      (Redshift TIMESTAMP has no timezone; TIMESTAMP_NTZ
       is the correct Snowflake equivalent)
    - SUPER → VARIANT
      (Snowflake's semi-structured type)

  WHERE DATA COMES FROM
  ----------------------
  - CONTROL.MIGRATION_TABLE_REGISTRY  (table list from Step 2)
  - CONTROL.TRANSLATION_CONFIG        (the translation rules)
  - Source DDL is read from REDSHIFT_MIRROR schema metadata

  PURPOSE
  -------
  Produce ready-to-execute CREATE TABLE statements for the
  TARGET schema. These are stored in DDL_TRANSLATION_LOG and
  used by the load step to create target tables before
  inserting data.

  DATABASE OBJECTS USED
  ----------------------
  - CONTROL.MIGRATION_TABLE_REGISTRY  (which tables to process)
  - CONTROL.TRANSLATION_CONFIG        (source → target rules)
  - REDSHIFT_MIRROR schema metadata   (source DDL)

  DATABASE OBJECTS AFFECTED
  --------------------------
  - TRANSLATION.DDL_TRANSLATION_LOG → 1 row per table with
    ORIGINAL_DDL and TRANSLATED_DDL populated
  - CONTROL.MIGRATION_TABLE_REGISTRY → STATUS updated to
    'DDL_TRANSLATED' for each processed table
  - CONTROL.PIPELINE_RUN_LOG → STARTED and SUCCESS rows logged
*/

-- VERIFY — Show the translation rules driving this step
SELECT
    CONFIG_TYPE,
    SOURCE_VALUE,
    TARGET_VALUE,
    NOTES
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.TRANSLATION_CONFIG
WHERE CONFIG_ID IN (
    SELECT MIN(CONFIG_ID)
    FROM MIGRATION_ACCELERATOR_DEV.CONTROL.TRANSLATION_CONFIG
    GROUP BY CONFIG_TYPE, SOURCE_VALUE
)
ORDER BY
    CONFIG_TYPE, SOURCE_VALUE;

/*
  EXPECTED RESULT
  ---------------
  Rows showing mappings like:
    DATA_TYPE  | TIMESTAMP   → TIMESTAMP_NTZ
    DATA_TYPE  | SUPER       → VARIANT
    DDL_CLAUSE | DISTKEY     → (empty — stripped)
    DDL_CLAUSE | SORTKEY     → (empty — stripped)
    DDL_CLAUSE | ENCODE      → (empty — stripped)
  These rows are what drive all DDL translation.
  Adding a new rule = inserting one row here. No code change.
*/

-- EXECUTE — Run DDL translation
CALL MIGRATION_ACCELERATOR_DEV.TRANSLATION.SP_TRANSLATE_DDL(
    $RUN_ID
);

-- VERIFY — Inspect original vs translated DDL for USERS table
SELECT
    SOURCE_TABLE,
    ORIGINAL_DDL,
    TRANSLATED_DDL
FROM
    MIGRATION_ACCELERATOR_DEV.TRANSLATION.DDL_TRANSLATION_LOG
WHERE
    SOURCE_TABLE = 'users'
    AND RUN_ID = $RUN_ID;

/*
  EXPECTED RESULT
  ---------------
  ORIGINAL_DDL will contain:
    ENCODE lzo / ENCODE az64 clauses on every column
    DISTSTYLE KEY
    DISTKEY (userid)
    SORTKEY (userid)

  TRANSLATED_DDL will be clean Snowflake DDL:
    No ENCODE clauses
    No DISTKEY / SORTKEY / DISTSTYLE
    TIMESTAMP → TIMESTAMP_NTZ where applicable
    CREATE OR REPLACE TABLE syntax
*/

-- VERIFY — Confirm all 7 tables were translated
SELECT
    SOURCE_TABLE,
    STATUS
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
WHERE
    RUN_ID = $RUN_ID
ORDER BY
    SOURCE_TABLE;

/*
  EXPECTED RESULT
  ---------------
  All 7 tables with STATUS = 'TRANSLATED'
*/


-- ============================================================
--  STEP 3B — CLASSIFY QUERIES BY MIGRATION COMPLEXITY
-- ============================================================
/*
  WHAT THIS STEP DOES
  -------------------
  Reads the Redshift query history and classifies each query
  into a workload category using Snowflake Cortex AI:

    REPORTING   — SELECT-heavy analytical queries, dashboards,
                  and aggregation patterns. Typically the largest
                  share of any Redshift workload.

    ETL         — INSERT/SELECT, COPY, CTAS, and data pipeline
                  queries that move or transform data between
                  tables or stages.

    AD_HOC      — One-off exploratory queries, data checks, or
                  developer/analyst investigations that are not
                  part of a scheduled workflow.

    MAINTENANCE — DDL operations, VACUUM, ANALYZE, permission
                  grants, and other housekeeping statements.

  This classification tells the migration team the composition
  of the SQL workload — how much is reporting vs. ETL vs.
  operational — so they can plan the translation effort by
  workload type rather than reviewing every query blindly.

  WHERE DATA COMES FROM
  ----------------------
  - Simulated query log stored in the TRANSLATION schema
    (in a real engagement this comes from Redshift's
    STL_QUERY or SVL_QLOG system tables)
  - Snowflake Cortex AI (mistral-large) for classification

  PURPOSE
  -------
  Give the migration team a clear breakdown of the SQL workload
  by type. ETL queries typically need the most rewriting for
  Snowflake compatibility, while REPORTING queries often
  translate with minimal changes.

  DATABASE OBJECTS USED
  ----------------------
  - TRANSLATION schema query log    (source queries)
  - CONTROL.MIGRATION_TABLE_REGISTRY (run context)
  - Snowflake Cortex (mistral-large) (AI classification)

  DATABASE OBJECTS AFFECTED
  --------------------------
  - TRANSLATION.QUERY_CLASSIFICATION_LOG → one row per query
    with QUERY_CATEGORY and CONFIDENCE assigned
  - CONTROL.PIPELINE_RUN_LOG → STARTED and SUCCESS rows logged
*/

-- EXECUTE — Classify queries
CALL MIGRATION_ACCELERATOR_DEV.TRANSLATION.SP_CLASSIFY_QUERIES(
    $RUN_ID
);

-- VERIFY — Show complexity distribution
SELECT
    QUERY_CATEGORY,
    COUNT(*) AS QUERY_COUNT
FROM
    MIGRATION_ACCELERATOR_DEV.TRANSLATION.QUERY_CLASSIFICATION_LOG
WHERE
    RUN_ID = $RUN_ID
GROUP BY
    QUERY_CATEGORY
ORDER BY
    QUERY_CATEGORY;

/*
  EXPECTED RESULT
  ---------------
  4 rows showing how many queries fall into each category:
    AD_HOC      | 1
    ETL         | 2
    MAINTENANCE | 1
    REPORTING   | 6

  In a real engagement, this distribution helps estimate
  the translation effort by query type — e.g. ETL queries
  often require the most rewriting for Snowflake compatibility.
*/


-- ============================================================
--  STEP 4 — LOAD DATA INTO TARGET TABLES
-- ============================================================
/*
  WHAT THIS STEP DOES
  -------------------
  The batch load controller reads the MIGRATION_TABLE_REGISTRY,
  picks up every table in DDL_TRANSLATED status, creates the
  target table in the TARGET schema using the translated DDL
  from Step 3, and then loads the data from REDSHIFT_MIRROR
  into the newly created TARGET table.

  It processes tables in batch and handles each one
  independently — so if one table fails, the others
  continue. Each table's status is updated as it progresses
  through LOADING → LOADED.

  WHERE DATA COMES FROM
  ----------------------
  - REDSHIFT_MIRROR.*              (source data, S3-backed)
  - TRANSLATION.DDL_TRANSLATION_LOG (translated CREATE TABLE
    statements from Step 3)

  PURPOSE
  -------
  Move the actual row data from the source (S3/Redshift mirror)
  into Snowflake's TARGET schema. After this step completes,
  every table exists in Snowflake with its full row count.

  DATABASE OBJECTS USED
  ----------------------
  - CONTROL.MIGRATION_TABLE_REGISTRY   (table list + status)
  - TRANSLATION.DDL_TRANSLATION_LOG    (translated DDL)
  - REDSHIFT_MIRROR.*                  (source data to load)

  DATABASE OBJECTS AFFECTED
  --------------------------
  - TARGET.CATEGORY   → created and loaded
  - TARGET.DATE       → created and loaded
  - TARGET.EVENT      → created and loaded
  - TARGET.LISTING    → created and loaded
  - TARGET.SALES      → created and loaded
  - TARGET.USERS      → created and loaded
  - TARGET.VENUE      → created and loaded
  - CONTROL.MIGRATION_TABLE_REGISTRY → STATUS updated to
    'LOADED' for each table
  - CONTROL.PIPELINE_RUN_LOG → STARTED + SUCCESS per table
*/

-- EXECUTE — Run batch load
CALL MIGRATION_ACCELERATOR_DEV.LOAD.SP_BATCH_LOAD_CONTROLLER(
    $RUN_ID
);

-- VERIFY — Confirm actual row counts in TARGET schema
SELECT 'CATEGORY' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM MIGRATION_ACCELERATOR_DEV.TARGET.CATEGORY
UNION ALL SELECT 'DATE',    COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.DATE
UNION ALL SELECT 'EVENT',   COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.EVENT
UNION ALL SELECT 'LISTING', COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.LISTING
UNION ALL SELECT 'SALES',   COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.SALES
UNION ALL SELECT 'USERS',   COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.USERS
UNION ALL SELECT 'VENUE',   COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.VENUE
ORDER BY TABLE_NAME;

/*
  EXPECTED RESULT
  ---------------
  TABLE_NAME  | ROW_COUNT
  CATEGORY    |        11
  DATE        |       365
  EVENT       |     8,798
  LISTING     |   192,497
  SALES       |   172,456
  USERS       |    49,990
  VENUE       |       187
  ---
  Total: ~624,304 rows loaded into Snowflake
*/

-- VERIFY — Confirm all tables reached LOADED status
SELECT
    SOURCE_TABLE,
    STATUS
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
WHERE
    RUN_ID = $RUN_ID
ORDER BY
    SOURCE_TABLE;

/*
  EXPECTED RESULT
  ---------------
  All 7 tables with STATUS = 'LOADED'
*/


-- ============================================================
--  STEP 5A — VALIDATE ROW COUNTS
-- ============================================================
/*
  WHAT THIS STEP DOES
  -------------------
  Compares the row count of every table between the source
  (REDSHIFT_MIRROR) and the target (TARGET schema in Snowflake).

  For each table it calculates:
    VARIANCE_PCT = ABS(target_count - source_count)
                   / source_count * 100

  This is checked against the tolerance threshold defined in
  VALIDATION_CONFIG. If variance is within threshold → PASS.
  If it exceeds threshold → FAIL.

  WHERE DATA COMES FROM
  ----------------------
  - REDSHIFT_MIRROR.*            (source row counts)
  - TARGET.*                     (post-load row counts)
  - CONTROL.VALIDATION_CONFIG    (acceptable variance threshold)

  PURPOSE
  -------
  Confirm that no rows were lost or duplicated during the
  load. This is the primary data integrity check — a migration
  is not considered complete until row counts pass.

  DATABASE OBJECTS USED
  ----------------------
  - REDSHIFT_MIRROR.*          (source count queries)
  - TARGET.*                   (target count queries)
  - CONTROL.VALIDATION_CONFIG  (threshold per check type)

  DATABASE OBJECTS AFFECTED
  --------------------------
  - CONTROL.VALIDATION_RESULTS → one row per table with
    SOURCE_VALUE, TARGET_VALUE, VARIANCE_PCT, and STATUS
  - CONTROL.PIPELINE_RUN_LOG   → STARTED and SUCCESS logged
*/

-- EXECUTE — Run row count validation across all tables
CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_ROW_COUNTS(
    $RUN_ID
);

-- VERIFY — Show row count comparison results
SELECT
    CHECK_NAME,
    SOURCE_VALUE::INTEGER AS SOURCE_VALUE,
    TARGET_VALUE::INTEGER AS TARGET_VALUE,
    VARIANCE_PCT,
    STATUS
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
WHERE
    RUN_ID = $RUN_ID
    AND CHECK_NAME NOT LIKE '%.%'
ORDER BY
    CHECK_NAME;

/*
  EXPECTED RESULT
  ---------------
  7 rows, one per table, all with STATUS = 'PASS'
  SOURCE_VALUE and TARGET_VALUE should match exactly.
  VARIANCE_PCT should be 0.00 for all tables.

  Any FAIL here would indicate rows were lost or duplicated
  during the load and must be investigated before go-live.
*/


-- ============================================================
--  STEP 5B — VALIDATE NULL RATES PER COLUMN
-- ============================================================
/*
  WHAT THIS STEP DOES
  -------------------
  For a specified table, calculates the null percentage for
  every column in both the source (REDSHIFT_MIRROR) and the
  target (TARGET schema). Then compares them.

  This catches a specific class of problem: a column that
  had 2% nulls in the source but now has 40% nulls in the
  target — indicating a data type conversion failure or a
  load error that silently coerced values to NULL.

  Row count checks alone would miss this. A table could pass
  row count validation but still have corrupted column data.
  Null rate checks close that gap.

  WHERE DATA COMES FROM
  ----------------------
  - REDSHIFT_MIRROR.[TABLE]     (source column null rates)
  - TARGET.[TABLE]              (target column null rates)
  - CONTROL.VALIDATION_CONFIG   (acceptable null rate delta)

  PURPOSE
  -------
  Detect column-level data quality issues introduced during
  type conversion or loading. Provides column-level PASS/FAIL
  that row count checks cannot provide.

  DATABASE OBJECTS USED
  ----------------------
  - REDSHIFT_MIRROR.USERS / SALES  (source null rate scan)
  - TARGET.USERS / SALES           (target null rate scan)
  - CONTROL.VALIDATION_CONFIG      (delta thresholds)

  DATABASE OBJECTS AFFECTED
  --------------------------
  - CONTROL.VALIDATION_RESULTS → multiple rows per table,
    one per column, each with SOURCE_NULL_PCT, TARGET_NULL_PCT,
    VARIANCE_PCT, and STATUS (PASS/FAIL)
  - CONTROL.PIPELINE_RUN_LOG   → STARTED and SUCCESS logged
*/

-- EXECUTE — Run null rate validation for USERS
CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_NULL_RATES(
    $RUN_ID,
    'USERS'
);

-- EXECUTE — Run null rate validation for SALES
CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_NULL_RATES(
    $RUN_ID,
    'SALES'
);

-- VERIFY — Show column-level null rate results
SELECT
    CHECK_NAME,
    SOURCE_VALUE::INTEGER AS SOURCE_VALUE,
    TARGET_VALUE::INTEGER AS TARGET_VALUE,
    VARIANCE_PCT,
    STATUS
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
WHERE
    RUN_ID = $RUN_ID
    AND CHECK_NAME LIKE '%.%'
ORDER BY
    CHECK_NAME;

/*
  EXPECTED RESULT
  ---------------
  Multiple rows — one per column per table.
  All STATUS = 'PASS'.
  SOURCE_VALUE and TARGET_VALUE should be identical or
  within the configured tolerance for each column.

  A FAIL on a boolean or timestamp column would suggest
  a type conversion issue introduced during translation.
*/

-- VERIFY — Full validation summary across all check types
SELECT
    CHECK_NAME,
    SOURCE_VALUE,
    TARGET_VALUE,
    VARIANCE_PCT,
    STATUS
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
WHERE
    RUN_ID = $RUN_ID
ORDER BY
    STATUS DESC,
    CHECK_NAME;

/*
  EXPECTED RESULT
  ---------------
  All rows with STATUS = 'PASS'.
  Any FAIL rows would appear at the top (ORDER BY STATUS DESC)
  and would require investigation before the migration
  is considered complete.
*/


-- ============================================================
--  STEP 6 — AUDIT TRAIL AND PIPELINE CLOSE
-- ============================================================
/*
  WHAT THIS STEP DOES
  -------------------
  Shows the complete execution history for this RUN_ID across
  all phases. Every procedure call wrote STARTED and SUCCESS
  (or FAILED) rows to PIPELINE_RUN_LOG as it ran. This step
  reads that log to provide a full audit trail.

  This is what a client, project manager, or auditor would
  look at to confirm the migration ran cleanly — what ran,
  in what order, how long each step took, and how many rows
  were processed.

  Finally, the MIGRATION_RUN record is updated to COMPLETED.

  WHERE DATA COMES FROM
  ----------------------
  - CONTROL.PIPELINE_RUN_LOG    (written by all procedures)
  - CONTROL.MIGRATION_RUN       (parent run record)

  PURPOSE
  -------
  Provide a full end-to-end record of everything that
  happened in this run. Then formally close the run by
  updating its status to COMPLETED.

  DATABASE OBJECTS USED
  ----------------------
  - CONTROL.PIPELINE_RUN_LOG    (read for audit display)
  - CONTROL.MIGRATION_RUN       (read + update)

  DATABASE OBJECTS AFFECTED
  --------------------------
  - CONTROL.MIGRATION_RUN → STATUS updated to 'COMPLETED'
*/

-- VERIFY — Full pipeline audit trail for this run
SELECT
    PHASE,
    STEP_NAME,
    STATUS,
    ROWS_PROCESSED,
    DURATION_SECS,
    ERROR_MESSAGE
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
WHERE
    RUN_ID = $RUN_ID
ORDER BY
    LOGGED_AT;

/*
  EXPECTED RESULT
  ---------------
  Every step across every phase, in chronological order:
    ASSESSMENT  | SP_ASSESS_SCHEMA         | STARTED
    ASSESSMENT  | SP_ASSESS_SCHEMA         | SUCCESS
    TRANSLATION | SP_TRANSLATE_DDL         | STARTED
    TRANSLATION | SP_TRANSLATE_DDL         | SUCCESS
    TRANSLATION | SP_CLASSIFY_QUERIES      | STARTED
    TRANSLATION | SP_CLASSIFY_QUERIES      | SUCCESS
    LOAD        | SP_BATCH_LOAD_CONTROLLER | STARTED
    LOAD        | SP_LOAD_TABLE (x7)       | SUCCESS per table
    LOAD        | SP_BATCH_LOAD_CONTROLLER | SUCCESS
    VALIDATION  | SP_VALIDATE_ROW_COUNTS   | STARTED
    VALIDATION  | SP_VALIDATE_ROW_COUNTS   | SUCCESS
    VALIDATION  | SP_VALIDATE_NULL_RATES   | STARTED (x2)
    VALIDATION  | SP_VALIDATE_NULL_RATES   | SUCCESS (x2)

  ERROR_MESSAGE should be NULL for all rows.
  This log is the complete, queryable record of the migration.
*/

-- EXECUTE — Close the run
UPDATE MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN
SET
    STATUS = 'COMPLETED'
WHERE
    RUN_ID = $RUN_ID;

-- VERIFY — Confirm run is marked COMPLETED
SELECT
    RUN_ID,
    RUN_NAME,
    STATUS,
    STARTED_AT
FROM
    MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN
WHERE
    RUN_ID = $RUN_ID;

/*
  EXPECTED RESULT
  ---------------
  STATUS = 'COMPLETED'

  The migration run is now fully closed and auditable.
  Open the Streamlit dashboard in Snowsight to see the
  same data visualized in the client-facing view:
    Snowsight → Projects → Streamlit
    → MIGRATION_ACCELERATOR_DASHBOARD
*/


-- ============================================================
--  END OF DEMO WALKTHROUGH SCRIPT
--
--  Summary of what was demonstrated:
--
--  Step 1  Create Run      CONTROL.MIGRATION_RUN
--  Step 2  Assessment      CONTROL.MIGRATION_TABLE_REGISTRY
--  Step 3a DDL Translation TRANSLATION.DDL_TRANSLATION_LOG
--  Step 3b Query Classify  TRANSLATION.QUERY_CLASSIFICATION
--  Step 4  Load            TARGET.* (all 7 tables, ~624K rows)
--  Step 5a Row Count Valid CONTROL.VALIDATION_RESULTS
--  Step 5b Null Rate Valid CONTROL.VALIDATION_RESULTS
--  Step 6  Audit + Close   CONTROL.PIPELINE_RUN_LOG
--
--  Every step was logged. Every result is queryable.
--  Nothing happened silently.
-- ============================================================