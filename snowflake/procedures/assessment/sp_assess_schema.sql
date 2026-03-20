-- =============================================================================
-- Procedure : SP_ASSESS_SCHEMA
-- Schema    : MIGRATION_ACCELERATOR_DEV.ASSESSMENT
-- Purpose   : Scans the REDSHIFT_MIRROR.SOURCE_TABLE_INVENTORY for a given
--             source schema and registers each discovered table into
--             CONTROL.MIGRATION_TABLE_REGISTRY with a PENDING status.
--             This is the first phase of the migration pipeline.
-- Parameters:
--   RUN_ID        – UUID of the current migration run (from CONTROL.MIGRATION_RUN)
--   SOURCE_SCHEMA – Redshift schema to assess (default: 'tickit')
-- Returns   : Success message with count of newly registered tables
-- Logging   : Writes STARTED / SUCCESS / FAILED to CONTROL.PIPELINE_RUN_LOG
-- =============================================================================

USE ROLE SYSADMIN;

USE DATABASE MIGRATION_ACCELERATOR_DEV;
USE SCHEMA ASSESSMENT;

CREATE OR REPLACE PROCEDURE SP_ASSESS_SCHEMA(
    RUN_ID VARCHAR,
    SOURCE_SCHEMA VARCHAR DEFAULT 'tickit'
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var startTime = Date.now();
    var rowsProcessed = 0;

    try {
        // Log the start of the assessment phase for audit traceability
        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS)
            VALUES
                ('${RUN_ID}', 'ASSESSMENT', 'SP_ASSESS_SCHEMA', 'STARTED')
        `});

        // Fetch all tables from the simulated Redshift metadata mirror.
        // In production, this would query Redshift system catalogs (pg_table_def,
        // SVV_TABLE_INFO) via JDBC external access integration.
        var inventoryStmt = snowflake.createStatement({sqlText: `
            SELECT SOURCE_SCHEMA, SOURCE_TABLE, ROW_COUNT_EST, SIZE_BYTES_EST
            FROM MIGRATION_ACCELERATOR_DEV.REDSHIFT_MIRROR.SOURCE_TABLE_INVENTORY
            WHERE UPPER(SOURCE_SCHEMA) = UPPER('${SOURCE_SCHEMA}')
        `});
        var inventoryResult = inventoryStmt.execute();

        // Iterate through each discovered source table and register it
        while (inventoryResult.next()) {
            var srcSchema = inventoryResult.getColumnValue('SOURCE_SCHEMA');
            var srcTable = inventoryResult.getColumnValue('SOURCE_TABLE');
            var rowCountEst = inventoryResult.getColumnValue('ROW_COUNT_EST');
            var sizeBytesEst = inventoryResult.getColumnValue('SIZE_BYTES_EST');
            var targetTable = srcTable.toUpperCase();  // Snowflake convention: uppercase table names

            // Idempotency check: skip if this table is already registered for this run
            var checkStmt = snowflake.createStatement({sqlText: `
                SELECT COUNT(*) AS CNT
                FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
                WHERE RUN_ID = '${RUN_ID}'
                  AND UPPER(SOURCE_SCHEMA) = UPPER('${srcSchema}')
                  AND UPPER(SOURCE_TABLE) = UPPER('${srcTable}')
            `});
            var checkResult = checkStmt.execute();
            checkResult.next();
            var existingCount = checkResult.getColumnValue('CNT');

            if (existingCount == 0) {
                // Register the table with PENDING status; downstream phases
                // (TRANSLATION, LOAD, VALIDATION) will update status as they process it
                snowflake.execute({sqlText: `
                    INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
                        (RUN_ID, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA, TARGET_TABLE, ROW_COUNT_EST, SIZE_BYTES_EST, STATUS)
                    VALUES
                        ('${RUN_ID}', '${srcSchema}', '${srcTable}', 'TARGET', '${targetTable}', ${rowCountEst}, ${sizeBytesEst}, 'PENDING')
                `});
                rowsProcessed++;
            }
        }

        // Calculate elapsed time and log successful completion
        var durationSecs = Math.round((Date.now() - startTime) / 1000);

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECS)
            VALUES
                ('${RUN_ID}', 'ASSESSMENT', 'SP_ASSESS_SCHEMA', 'SUCCESS', ${rowsProcessed}, ${durationSecs})
        `});

        return 'SUCCESS: Registered ' + rowsProcessed + ' tables for schema ' + SOURCE_SCHEMA;

    } catch (err) {
        // Log failure with error detail for debugging; escape single quotes to
        // prevent SQL injection in the error message string
        var durationSecs = Math.round((Date.now() - startTime) / 1000);
        var errorMsg = err.message.replace(/'/g, "''");

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, DURATION_SECS, ERROR_MESSAGE)
            VALUES
                ('${RUN_ID}', 'ASSESSMENT', 'SP_ASSESS_SCHEMA', 'FAILED', ${durationSecs}, '${errorMsg}')
        `});

        throw err;  // Re-throw so the caller sees the failure
    }
$$;


-- =============================================================================
-- MANUAL TESTING STEPS (uncomment and run sequentially)
-- =============================================================================

-- -- Step 1: Create a test run record in the control table
-- INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN 
--     (RUN_NAME, STATUS)
-- VALUES 
--     ('POC Test Run 1', 'STARTED');

-- -- Grab the RUN_ID that was just created
-- SELECT RUN_ID FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN 
-- WHERE RUN_NAME = 'POC Test Run 1';

-- -- Call the procedure (replace <RUN_ID> with the value above)
-- CALL MIGRATION_ACCELERATOR_DEV.ASSESSMENT.SP_ASSESS_SCHEMA(
--     'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a', 
--     'tickit'
-- );

-- -- Verify registry was populated
-- SELECT SOURCE_TABLE, TARGET_TABLE, STATUS
-- FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
-- ORDER BY SOURCE_TABLE;

-- -- Verify log was written
-- SELECT PHASE, STEP_NAME, STATUS, ROWS_PROCESSED
-- FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
-- ORDER BY LOGGED_AT;