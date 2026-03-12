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
        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS)
            VALUES
                ('${RUN_ID}', 'ASSESSMENT', 'SP_ASSESS_SCHEMA', 'STARTED')
        `});

        var inventoryStmt = snowflake.createStatement({sqlText: `
            SELECT SOURCE_SCHEMA, SOURCE_TABLE, ROW_COUNT_EST, SIZE_BYTES_EST
            FROM MIGRATION_ACCELERATOR_DEV.REDSHIFT_MIRROR.SOURCE_TABLE_INVENTORY
            WHERE UPPER(SOURCE_SCHEMA) = UPPER('${SOURCE_SCHEMA}')
        `});
        var inventoryResult = inventoryStmt.execute();

        while (inventoryResult.next()) {
            var srcSchema = inventoryResult.getColumnValue('SOURCE_SCHEMA');
            var srcTable = inventoryResult.getColumnValue('SOURCE_TABLE');
            var rowCountEst = inventoryResult.getColumnValue('ROW_COUNT_EST');
            var sizeBytesEst = inventoryResult.getColumnValue('SIZE_BYTES_EST');
            var targetTable = srcTable.toUpperCase();

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
                snowflake.execute({sqlText: `
                    INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
                        (RUN_ID, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA, TARGET_TABLE, ROW_COUNT_EST, SIZE_BYTES_EST, STATUS)
                    VALUES
                        ('${RUN_ID}', '${srcSchema}', '${srcTable}', 'TARGET', '${targetTable}', ${rowCountEst}, ${sizeBytesEst}, 'PENDING')
                `});
                rowsProcessed++;
            }
        }

        var durationSecs = Math.round((Date.now() - startTime) / 1000);

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECS)
            VALUES
                ('${RUN_ID}', 'ASSESSMENT', 'SP_ASSESS_SCHEMA', 'SUCCESS', ${rowsProcessed}, ${durationSecs})
        `});

        return 'SUCCESS: Registered ' + rowsProcessed + ' tables for schema ' + SOURCE_SCHEMA;

    } catch (err) {
        var durationSecs = Math.round((Date.now() - startTime) / 1000);
        var errorMsg = err.message.replace(/'/g, "''");

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, DURATION_SECS, ERROR_MESSAGE)
            VALUES
                ('${RUN_ID}', 'ASSESSMENT', 'SP_ASSESS_SCHEMA', 'FAILED', ${durationSecs}, '${errorMsg}')
        `});

        throw err;
    }
$$;


-- First create a test run record
INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN 
    (RUN_NAME, STATUS)
VALUES 
    ('POC Test Run 1', 'STARTED');

-- Grab the RUN_ID that was just created
SELECT RUN_ID FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN 
WHERE RUN_NAME = 'POC Test Run 1';

-- Call the procedure (replace <RUN_ID> with the value above)
CALL MIGRATION_ACCELERATOR_DEV.ASSESSMENT.SP_ASSESS_SCHEMA(
    'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a', 
    'tickit'
);

-- Verify registry was populated
SELECT SOURCE_TABLE, TARGET_TABLE, STATUS
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
ORDER BY SOURCE_TABLE;

-- Verify log was written
SELECT PHASE, STEP_NAME, STATUS, ROWS_PROCESSED
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
ORDER BY LOGGED_AT;