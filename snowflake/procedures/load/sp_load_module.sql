-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 1 — File Format
-- ═══════════════════════════════════════════════════════════════════════════

USE ROLE SYSADMIN;

USE DATABASE MIGRATION_ACCELERATOR_DEV;
USE SCHEMA STAGING;

CREATE FILE FORMAT IF NOT EXISTS TICKIT_PIPE_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 0
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE;

CREATE FILE FORMAT IF NOT EXISTS TICKIT_TAB_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = '\t'
    SKIP_HEADER = 0
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE;

-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 2 — External Stage
-- ═══════════════════════════════════════════════════════════════════════════

-- CREATE STAGE IF NOT EXISTS TICKIT_S3_STAGE
--     URL = 's3://redshift-snowflake-accelerator-tickitdb-sample-data/tickit/'
--     FILE_FORMAT = MIGRATION_ACCELERATOR_DEV.STAGING.TICKIT_PIPE_FORMAT
--     COMMENT = 'Public AWS TICKIT sample dataset';

USE ROLE ACCOUNTADMIN;

-- Check current setting
SHOW PARAMETERS LIKE 'REQUIRE_STORAGE_INTEGRATION%' IN ACCOUNT;

-- Disable the restriction (allows direct S3 URLs without storage integration)
ALTER ACCOUNT SET REQUIRE_STORAGE_INTEGRATION_FOR_STAGE_CREATION = FALSE;
ALTER ACCOUNT SET REQUIRE_STORAGE_INTEGRATION_FOR_STAGE_OPERATION = FALSE;

-- CREATE STAGE IF NOT EXISTS TICKIT_S3_STAGE
CREATE OR REPLACE STAGE TICKIT_S3_STAGE
    URL = 's3://awssampledbuswest2/tickit/'
    FILE_FORMAT = MIGRATION_ACCELERATOR_DEV.STAGING.TICKIT_PIPE_FORMAT
    COMMENT = 'Public AWS TICKIT sample dataset';






-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 3 — SP_LOAD_TABLE procedure
-- ═══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.LOAD;

USE SCHEMA LOAD;

CREATE OR REPLACE PROCEDURE SP_LOAD_TABLE(
    RUN_ID VARCHAR,
    SOURCE_TABLE VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var startTime = Date.now();
    var rowCount = 0;

    var fileMapping = {
        'users':    'allusers_pipe.txt',
        'venue':    'venue_pipe.txt',
        'category': 'category_pipe.txt',
        'date':     'date2008_pipe.txt',
        'event':    'allevents_pipe.txt',
        'listing':  'listings_pipe.txt',
        'sales':    'sales_tab.txt'
    };

    try {
        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS)
            VALUES
                ('${RUN_ID}', 'LOAD', 'SP_LOAD_TABLE', 'STARTED')
        `});

        var ddlStmt = snowflake.createStatement({sqlText: `
            SELECT TRANSLATED_DDL
            FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.DDL_TRANSLATION_LOG
            WHERE UPPER(SOURCE_TABLE) = UPPER('${SOURCE_TABLE}')
            ORDER BY TRANSLATED_AT DESC
            LIMIT 1
        `});
        var ddlResult = ddlStmt.execute();

        if (!ddlResult.next()) {
            throw new Error('No translated DDL found for table: ' + SOURCE_TABLE);
        }

        var translatedDdl = ddlResult.getColumnValue('TRANSLATED_DDL');
        var targetTable = SOURCE_TABLE.toUpperCase();

        if (translatedDdl.indexOf('MIGRATION_ACCELERATOR_DEV.TARGET.') === -1) {
            translatedDdl = translatedDdl.replace(
                /CREATE\s+OR\s+REPLACE\s+TABLE\s+(\w+)/i,
                'CREATE OR REPLACE TABLE MIGRATION_ACCELERATOR_DEV.TARGET.' + targetTable
            );
        }

        snowflake.execute({sqlText: translatedDdl});

        var sourceTableLower = SOURCE_TABLE.toLowerCase();
        var fileName = fileMapping[sourceTableLower];

        if (!fileName) {
            throw new Error('No file mapping found for table: ' + SOURCE_TABLE);
        }

        var fileFormat = 'MIGRATION_ACCELERATOR_DEV.STAGING.TICKIT_PIPE_FORMAT';
        if (sourceTableLower === 'sales') {
            fileFormat = 'MIGRATION_ACCELERATOR_DEV.STAGING.TICKIT_TAB_FORMAT';
        }

        var copyStmt = `
            COPY INTO MIGRATION_ACCELERATOR_DEV.TARGET.${targetTable}
            FROM @MIGRATION_ACCELERATOR_DEV.STAGING.TICKIT_S3_STAGE/${fileName}
            FILE_FORMAT = (FORMAT_NAME = '${fileFormat}')
            ON_ERROR = 'CONTINUE'
        `;
        snowflake.execute({sqlText: copyStmt});

        var countStmt = snowflake.createStatement({sqlText: `
            SELECT COUNT(*) AS CNT FROM MIGRATION_ACCELERATOR_DEV.TARGET.${targetTable}
        `});
        var countResult = countStmt.execute();
        countResult.next();
        rowCount = countResult.getColumnValue('CNT');

        snowflake.execute({sqlText: `
            UPDATE MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
            SET STATUS = 'LOADED', UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE RUN_ID = '${RUN_ID}'
              AND UPPER(SOURCE_TABLE) = UPPER('${SOURCE_TABLE}')
        `});

        var durationSecs = Math.round((Date.now() - startTime) / 1000);

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECS)
            VALUES
                ('${RUN_ID}', 'LOAD', 'SP_LOAD_TABLE', 'SUCCESS', ${rowCount}, ${durationSecs})
        `});

        return 'SUCCESS: Loaded ' + rowCount + ' rows into TARGET.' + targetTable;

    } catch (err) {
        var durationSecs = Math.round((Date.now() - startTime) / 1000);
        var errorMsg = err.message.replace(/'/g, "''");

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, DURATION_SECS, ERROR_MESSAGE)
            VALUES
                ('${RUN_ID}', 'LOAD', 'SP_LOAD_TABLE', 'FAILED', ${durationSecs}, '${errorMsg}')
        `});

        throw err;
    }
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 4 — SP_BATCH_LOAD_CONTROLLER procedure
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE SP_BATCH_LOAD_CONTROLLER(
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var startTime = Date.now();
    var tablesLoaded = 0;
    var tablesFailed = 0;
    var failedTables = [];

    try {
        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS)
            VALUES
                ('${RUN_ID}', 'LOAD', 'SP_BATCH_LOAD_CONTROLLER', 'STARTED')
        `});

        var tableStmt = snowflake.createStatement({sqlText: `
            SELECT SOURCE_TABLE
            FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
            WHERE RUN_ID = '${RUN_ID}'
              AND STATUS = 'TRANSLATED'
            ORDER BY PRIORITY ASC
        `});
        var tableResult = tableStmt.execute();

        while (tableResult.next()) {
            var sourceTable = tableResult.getColumnValue('SOURCE_TABLE');

            try {
                snowflake.execute({sqlText: `
                    CALL MIGRATION_ACCELERATOR_DEV.LOAD.SP_LOAD_TABLE('${RUN_ID}', '${sourceTable}')
                `});
                tablesLoaded++;
            } catch (loadErr) {
                tablesFailed++;
                failedTables.push(sourceTable + ': ' + loadErr.message);
            }
        }

        var durationSecs = Math.round((Date.now() - startTime) / 1000);

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECS)
            VALUES
                ('${RUN_ID}', 'LOAD', 'SP_BATCH_LOAD_CONTROLLER', 'SUCCESS', ${tablesLoaded}, ${durationSecs})
        `});

        var result = 'SUCCESS: Loaded ' + tablesLoaded + ' tables';
        if (tablesFailed > 0) {
            result += ', ' + tablesFailed + ' failed: ' + failedTables.join('; ');
        }
        return result;

    } catch (err) {
        var durationSecs = Math.round((Date.now() - startTime) / 1000);
        var errorMsg = err.message.replace(/'/g, "''");

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, DURATION_SECS, ERROR_MESSAGE)
            VALUES
                ('${RUN_ID}', 'LOAD', 'SP_BATCH_LOAD_CONTROLLER', 'FAILED', ${durationSecs}, '${errorMsg}')
        `});

        throw err;
    }
$$;


--

-- Run the full batch load
CALL MIGRATION_ACCELERATOR_DEV.LOAD.SP_BATCH_LOAD_CONTROLLER(
    'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a'
);

-- Check what landed in TARGET schema
SHOW TABLES IN SCHEMA MIGRATION_ACCELERATOR_DEV.TARGET;

-- Check row counts per table
SELECT SOURCE_TABLE, STATUS
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
WHERE RUN_ID = 'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a'
ORDER BY SOURCE_TABLE;

-- Spot check one table
SELECT COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.USERS;
SELECT COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.VENUE;
SELECT COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.CATEGORY;
SELECT COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.DATE;
SELECT COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.EVENT;
SELECT COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.LISTING;
SELECT COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.SALES;

-- Check full pipeline log
SELECT PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, ERROR_MESSAGE
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
ORDER BY LOGGED_AT;