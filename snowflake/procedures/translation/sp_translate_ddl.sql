USE ROLE SYSADMIN;

USE DATABASE MIGRATION_ACCELERATOR_DEV;
USE SCHEMA TRANSLATION;

CREATE OR REPLACE PROCEDURE SP_TRANSLATE_DDL(
    RUN_ID VARCHAR
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
                ('${RUN_ID}', 'TRANSLATION', 'SP_TRANSLATE_DDL', 'STARTED')
        `});

        snowflake.execute({sqlText: `
            CREATE TABLE IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.TRANSLATION.DDL_TRANSLATION_LOG (
                TRANSLATION_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
                RUN_ID VARCHAR(36),
                SOURCE_SCHEMA VARCHAR(255),
                SOURCE_TABLE VARCHAR(255),
                ORIGINAL_DDL VARCHAR(16777216),
                TRANSLATED_DDL VARCHAR(16777216),
                TRANSLATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        `});

        var dataTypeMaps = [];
        var dtStmt = snowflake.createStatement({sqlText: `
            SELECT SOURCE_VALUE, TARGET_VALUE, IS_REGEX
            FROM MIGRATION_ACCELERATOR_DEV.CONTROL.TRANSLATION_CONFIG
            WHERE CONFIG_TYPE = 'DATA_TYPE_MAP' AND IS_ACTIVE = TRUE
            ORDER BY PRIORITY
        `});
        var dtResult = dtStmt.execute();
        while (dtResult.next()) {
            dataTypeMaps.push({
                source: dtResult.getColumnValue('SOURCE_VALUE'),
                target: dtResult.getColumnValue('TARGET_VALUE'),
                isRegex: dtResult.getColumnValue('IS_REGEX')
            });
        }

        var clauseStrips = [];
        var csStmt = snowflake.createStatement({sqlText: `
            SELECT SOURCE_VALUE
            FROM MIGRATION_ACCELERATOR_DEV.CONTROL.TRANSLATION_CONFIG
            WHERE CONFIG_TYPE = 'CLAUSE_STRIP' AND IS_ACTIVE = TRUE
            ORDER BY PRIORITY
        `});
        var csResult = csStmt.execute();
        while (csResult.next()) {
            clauseStrips.push(csResult.getColumnValue('SOURCE_VALUE'));
        }

        var functionMaps = [];
        var fmStmt = snowflake.createStatement({sqlText: `
            SELECT SOURCE_VALUE, TARGET_VALUE, IS_REGEX
            FROM MIGRATION_ACCELERATOR_DEV.CONTROL.TRANSLATION_CONFIG
            WHERE CONFIG_TYPE = 'FUNCTION_MAP' AND IS_ACTIVE = TRUE
            ORDER BY PRIORITY
        `});
        var fmResult = fmStmt.execute();
        while (fmResult.next()) {
            functionMaps.push({
                source: fmResult.getColumnValue('SOURCE_VALUE'),
                target: fmResult.getColumnValue('TARGET_VALUE'),
                isRegex: fmResult.getColumnValue('IS_REGEX')
            });
        }

        var ddlStmt = snowflake.createStatement({sqlText: `
            SELECT SOURCE_SCHEMA, SOURCE_TABLE, ORIGINAL_DDL
            FROM MIGRATION_ACCELERATOR_DEV.REDSHIFT_MIRROR.SOURCE_DDL_STORE
        `});
        var ddlResult = ddlStmt.execute();

        while (ddlResult.next()) {
            var srcSchema = ddlResult.getColumnValue('SOURCE_SCHEMA');
            var srcTable = ddlResult.getColumnValue('SOURCE_TABLE');
            var originalDdl = ddlResult.getColumnValue('ORIGINAL_DDL');
            var translatedDdl = originalDdl;

            for (var i = 0; i < dataTypeMaps.length; i++) {
                var map = dataTypeMaps[i];
                var wordBoundaryPattern = '\\b' + map.source + '\\b';
                translatedDdl = translatedDdl.replace(new RegExp(wordBoundaryPattern, 'gi'), map.target);
            }

            for (var j = 0; j < clauseStrips.length; j++) {
                var pattern = clauseStrips[j];
                translatedDdl = translatedDdl.replace(new RegExp(pattern, 'gi'), '');
            }

            for (var k = 0; k < functionMaps.length; k++) {
                var fmap = functionMaps[k];
                if (fmap.isRegex) {
                    translatedDdl = translatedDdl.replace(new RegExp(fmap.source, 'gi'), fmap.target);
                } else {
                    translatedDdl = translatedDdl.split(fmap.source).join(fmap.target);
                }
            }

            translatedDdl = translatedDdl.replace(/ENCODE\s+\w+/gi, '');
            translatedDdl = translatedDdl.replace(/DISTSTYLE\s+(KEY|ALL|EVEN)/gi, '');
            translatedDdl = translatedDdl.replace(/DISTKEY\s*\([^)]+\)/gi, '');
            translatedDdl = translatedDdl.replace(/SORTKEY\s*\([^)]+\)/gi, '');
            translatedDdl = translatedDdl.replace(/tickit\./gi, '');
            translatedDdl = translatedDdl.replace(/CREATE\s+TABLE/gi, 'CREATE OR REPLACE TABLE');
            translatedDdl = translatedDdl.replace(/,\s*\n\s*\)/g, '\n)');
            translatedDdl = translatedDdl.replace(/\n\s*\n/g, '\n');

            var escapedOriginal = originalDdl.replace(/'/g, "''");
            var escapedTranslated = translatedDdl.replace(/'/g, "''");

            snowflake.execute({sqlText: `
                INSERT INTO MIGRATION_ACCELERATOR_DEV.TRANSLATION.DDL_TRANSLATION_LOG
                    (RUN_ID, SOURCE_SCHEMA, SOURCE_TABLE, ORIGINAL_DDL, TRANSLATED_DDL)
                VALUES
                    ('${RUN_ID}', '${srcSchema}', '${srcTable}', '${escapedOriginal}', '${escapedTranslated}')
            `});

            snowflake.execute({sqlText: `
                UPDATE MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
                SET STATUS = 'TRANSLATED', UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RUN_ID = '${RUN_ID}'
                  AND UPPER(SOURCE_SCHEMA) = UPPER('${srcSchema}')
                  AND UPPER(SOURCE_TABLE) = UPPER('${srcTable}')
            `});

            rowsProcessed++;
        }

        var durationSecs = Math.round((Date.now() - startTime) / 1000);

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECS)
            VALUES
                ('${RUN_ID}', 'TRANSLATION', 'SP_TRANSLATE_DDL', 'SUCCESS', ${rowsProcessed}, ${durationSecs})
        `});

        return 'SUCCESS: Translated ' + rowsProcessed + ' DDL statements';

    } catch (err) {
        var durationSecs = Math.round((Date.now() - startTime) / 1000);
        var errorMsg = err.message.replace(/'/g, "''");

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, DURATION_SECS, ERROR_MESSAGE)
            VALUES
                ('${RUN_ID}', 'TRANSLATION', 'SP_TRANSLATE_DDL', 'FAILED', ${durationSecs}, '${errorMsg}')
        `});

        throw err;
    }
$$;


-- Grab your RUN_ID
SELECT RUN_ID FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN
WHERE RUN_NAME = 'POC Test Run 1';

-- Call the procedure (replace <RUN_ID>)
CALL MIGRATION_ACCELERATOR_DEV.TRANSLATION.SP_TRANSLATE_DDL('f71a5e9c-3ec7-4a33-ae84-a0cda464d63a');

-- Verify translated DDL looks clean
SELECT 
    SOURCE_TABLE,
    TRANSLATED_DDL
FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.DDL_TRANSLATION_LOG
ORDER BY SOURCE_TABLE;

-- Verify status updated
SELECT SOURCE_TABLE, STATUS
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
ORDER BY SOURCE_TABLE;

-- Verify log
SELECT PHASE, STEP_NAME, STATUS, ROWS_PROCESSED
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
ORDER BY LOGGED_AT;

DELETE FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.DDL_TRANSLATION_LOG;

UPDATE MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
SET STATUS = 'PENDING';
