-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 1 — SP_VALIDATE_ROW_COUNTS
-- ═══════════════════════════════════════════════════════════════════════════

USE ROLE SYSADMIN;

USE DATABASE MIGRATION_ACCELERATOR_DEV;
USE SCHEMA VALIDATION;

CREATE OR REPLACE PROCEDURE SP_VALIDATE_ROW_COUNTS(
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var startTime = Date.now();
    var tablesValidated = 0;

    try {
        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS)
            VALUES
                ('${RUN_ID}', 'VALIDATION', 'SP_VALIDATE_ROW_COUNTS', 'STARTED')
        `});

        var tableStmt = snowflake.createStatement({sqlText: `
            SELECT SOURCE_SCHEMA, SOURCE_TABLE, TARGET_TABLE
            FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
            WHERE RUN_ID = '${RUN_ID}'
              AND STATUS = 'LOADED'
        `});
        var tableResult = tableStmt.execute();

        while (tableResult.next()) {
            var sourceSchema = tableResult.getColumnValue('SOURCE_SCHEMA');
            var sourceTable = tableResult.getColumnValue('SOURCE_TABLE');
            var targetTable = tableResult.getColumnValue('TARGET_TABLE');

            var srcCountStmt = snowflake.createStatement({sqlText: `
                SELECT ROW_COUNT_EST
                FROM MIGRATION_ACCELERATOR_DEV.REDSHIFT_MIRROR.SOURCE_TABLE_INVENTORY
                WHERE UPPER(SOURCE_TABLE) = UPPER('${sourceTable}')
                LIMIT 1
            `});
            var srcCountResult = srcCountStmt.execute();
            var sourceCount = 0;
            if (srcCountResult.next()) {
                sourceCount = srcCountResult.getColumnValue('ROW_COUNT_EST') || 0;
            }

            var tgtCountStmt = snowflake.createStatement({sqlText: `
                SELECT COUNT(*) AS CNT FROM MIGRATION_ACCELERATOR_DEV.TARGET.${targetTable}
            `});
            var tgtCountResult = tgtCountStmt.execute();
            tgtCountResult.next();
            var targetCount = tgtCountResult.getColumnValue('CNT');

            var variancePct = 0;
            if (sourceCount > 0) {
                variancePct = Math.abs(sourceCount - targetCount) / sourceCount * 100;
            }
            var varianceRounded = Math.round(variancePct * 10000) / 10000;

            var checkResult = 'PASS';
            var notes = 'Within 5% threshold';
            if (variancePct > 5.0) {
                checkResult = 'FAIL';
                notes = 'Exceeds 5% threshold';
            }

            snowflake.execute({sqlText: `
                INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
                    (RUN_ID, CHECK_NAME, SOURCE_VALUE, TARGET_VALUE, VARIANCE_PCT, STATUS)
                SELECT
                    '${RUN_ID}', 'ROW_COUNT:${sourceTable}', TO_VARIANT('${sourceCount}'), TO_VARIANT('${targetCount}'), ${varianceRounded}, '${checkResult}'
            `});

            tablesValidated++;
        }

        snowflake.execute({sqlText: `
            UPDATE MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
            SET STATUS = 'VALIDATED', UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE RUN_ID = '${RUN_ID}'
              AND STATUS = 'LOADED'
        `});

        var durationSecs = Math.round((Date.now() - startTime) / 1000);

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECS)
            VALUES
                ('${RUN_ID}', 'VALIDATION', 'SP_VALIDATE_ROW_COUNTS', 'SUCCESS', ${tablesValidated}, ${durationSecs})
        `});

        return 'SUCCESS: Validated row counts for ' + tablesValidated + ' tables';

    } catch (err) {
        var durationSecs = Math.round((Date.now() - startTime) / 1000);
        var errorMsg = err.message.replace(/'/g, "''");

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, DURATION_SECS, ERROR_MESSAGE)
            VALUES
                ('${RUN_ID}', 'VALIDATION', 'SP_VALIDATE_ROW_COUNTS', 'FAILED', ${durationSecs}, '${errorMsg}')
        `});

        throw err;
    }
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 2 — SP_VALIDATE_NULL_RATES
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE SP_VALIDATE_NULL_RATES(
    RUN_ID VARCHAR,
    TABLE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var startTime = Date.now();
    var columnsChecked = 0;

    try {
        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS)
            VALUES
                ('${RUN_ID}', 'VALIDATION', 'SP_VALIDATE_NULL_RATES', 'STARTED')
        `});

        var thresholdStmt = snowflake.createStatement({sqlText: `
            SELECT FAIL_THRESHOLD
            FROM MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_CONFIG
            WHERE CHECK_TYPE = 'NULL_RATE' AND IS_ACTIVE = TRUE
            LIMIT 1
        `});
        var thresholdResult = thresholdStmt.execute();
        var failThreshold = 1;
        if (thresholdResult.next()) {
            failThreshold = thresholdResult.getColumnValue('FAIL_THRESHOLD') || 1;
        }

        var colStmt = snowflake.createStatement({sqlText: `
            SELECT COLUMN_NAME
            FROM MIGRATION_ACCELERATOR_DEV.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'TARGET'
              AND UPPER(TABLE_NAME) = UPPER('${TABLE_NAME}')
            ORDER BY ORDINAL_POSITION
        `});
        var colResult = colStmt.execute();

        while (colResult.next()) {
            var columnName = colResult.getColumnValue('COLUMN_NAME');

            var sourceStmt = snowflake.createStatement({sqlText: `
                SELECT
                    COUNT_IF(${columnName} IS NULL) / NULLIF(COUNT(*), 0) * 100 AS NULL_RATE
                FROM MIGRATION_ACCELERATOR_DEV.STAGING.${TABLE_NAME}
            `});
            var sourceResult = sourceStmt.execute();
            sourceResult.next();
            var sourceRate = sourceResult.getColumnValue('NULL_RATE') || 0;
            var sourceRateRounded = Math.round(sourceRate * 10000) / 10000;

            var targetStmt = snowflake.createStatement({sqlText: `
                SELECT
                    COUNT_IF(${columnName} IS NULL) / NULLIF(COUNT(*), 0) * 100 AS NULL_RATE
                FROM MIGRATION_ACCELERATOR_DEV.TARGET.${TABLE_NAME}
            `});
            var targetResult = targetStmt.execute();
            targetResult.next();
            var targetRate = targetResult.getColumnValue('NULL_RATE') || 0;
            var targetRateRounded = Math.round(targetRate * 10000) / 10000;

            var delta = Math.abs(targetRateRounded - sourceRateRounded);
            var deltaRounded = Math.round(delta * 10000) / 10000;

            var checkResult = 'PASS';
            if (deltaRounded > failThreshold) {
                checkResult = 'FAIL';
            }

            snowflake.execute({sqlText: `
                INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
                    (RUN_ID, CHECK_NAME, SOURCE_VALUE, TARGET_VALUE, VARIANCE_PCT, STATUS)
                SELECT
                    '${RUN_ID}',
                    'NULL_RATE:${TABLE_NAME}.${columnName}',
                    TO_VARIANT('${sourceRateRounded}%'),
                    TO_VARIANT('${targetRateRounded}%'),
                    ${deltaRounded},
                    '${checkResult}'
            `});

            columnsChecked++;
        }

        var durationSecs = Math.round((Date.now() - startTime) / 1000);

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECS)
            VALUES
                ('${RUN_ID}', 'VALIDATION', 'SP_VALIDATE_NULL_RATES', 'SUCCESS', ${columnsChecked}, ${durationSecs})
        `});

        return 'SUCCESS: Checked null rates for ' + columnsChecked + ' columns in ' + TABLE_NAME;

    } catch (err) {
        var durationSecs = Math.round((Date.now() - startTime) / 1000);
        var errorMsg = err.message.replace(/'/g, "''");

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, DURATION_SECS, ERROR_MESSAGE)
            VALUES
                ('${RUN_ID}', 'VALIDATION', 'SP_VALIDATE_NULL_RATES', 'FAILED', ${durationSecs}, '${errorMsg}')
        `});

        throw err;
    }
$$;


--

-- Run row count validation
CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_ROW_COUNTS(
    'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a'
);

-- Run null rate validation for two tables
CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_NULL_RATES(
    'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a',
    'USERS'
);

CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_NULL_RATES(
    'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a',
    'SALES'
);

-- Check validation results
SELECT 
    CHECK_NAME,
    SOURCE_VALUE,
    TARGET_VALUE,
    VARIANCE_PCT,
    STATUS
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
WHERE RUN_ID = 'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a'
ORDER BY CHECK_NAME;

-- Check all tables reached VALIDATED status
SELECT SOURCE_TABLE, STATUS
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
WHERE RUN_ID = 'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a'
ORDER BY SOURCE_TABLE;

-- Check pipeline log
SELECT PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, ERROR_MESSAGE
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
ORDER BY LOGGED_AT;