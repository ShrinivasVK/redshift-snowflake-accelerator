USE ROLE SYSADMIN;

USE DATABASE MIGRATION_ACCELERATOR_DEV;
USE SCHEMA TRANSLATION;

CREATE OR REPLACE PROCEDURE SP_CLASSIFY_QUERIES(
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var startTime = Date.now();
    var rowsProcessed = 0;
    var validCategories = ['ETL', 'REPORTING', 'AD_HOC', 'MAINTENANCE'];

    try {
        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS)
            VALUES
                ('${RUN_ID}', 'TRANSLATION', 'SP_CLASSIFY_QUERIES', 'STARTED')
        `});

        snowflake.execute({sqlText: `
            CREATE TABLE IF NOT EXISTS MIGRATION_ACCELERATOR_DEV.TRANSLATION.QUERY_CLASSIFICATION_LOG (
                CLASSIFICATION_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
                RUN_ID VARCHAR(36),
                QUERY_TEXT VARCHAR(16777216),
                QUERY_CATEGORY VARCHAR(100),
                CONFIDENCE VARCHAR(20),
                CLASSIFIED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        `});

        snowflake.execute({sqlText: `
            CREATE OR REPLACE TEMPORARY TABLE MIGRATION_ACCELERATOR_DEV.TRANSLATION.TEMP_SAMPLE_QUERIES (
                QUERY_ID INT,
                QUERY_TEXT VARCHAR(16777216)
            )
        `});

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.TRANSLATION.TEMP_SAMPLE_QUERIES (QUERY_ID, QUERY_TEXT) VALUES
            (1, 'INSERT INTO analytics.daily_sales SELECT saletime::DATE as sale_date, SUM(pricepaid) as total_sales, COUNT(*) as num_transactions FROM tickit.sales GROUP BY saletime::DATE'),
            (2, 'CREATE TABLE analytics.event_summary AS SELECT e.eventid, e.eventname, v.venuename, COUNT(s.salesid) as total_sales FROM tickit.event e JOIN tickit.venue v ON e.venueid = v.venueid LEFT JOIN tickit.sales s ON e.eventid = s.eventid GROUP BY e.eventid, e.eventname, v.venuename'),
            (3, 'SELECT c.catname, COUNT(e.eventid) as num_events, SUM(s.pricepaid) as revenue FROM tickit.category c JOIN tickit.event e ON c.catid = e.catid JOIN tickit.sales s ON e.eventid = s.eventid GROUP BY c.catname ORDER BY revenue DESC'),
            (4, 'SELECT DATE_TRUNC(''month'', saletime) as month, SUM(pricepaid) as monthly_revenue, SUM(commission) as monthly_commission FROM tickit.sales GROUP BY DATE_TRUNC(''month'', saletime) ORDER BY month'),
            (5, 'SELECT v.venuestate, COUNT(DISTINCT e.eventid) as events, SUM(s.qtysold) as tickets_sold FROM tickit.venue v JOIN tickit.event e ON v.venueid = e.venueid JOIN tickit.sales s ON e.eventid = s.eventid GROUP BY v.venuestate'),
            (6, 'SELECT * FROM tickit.users WHERE state = ''CA'' AND likeconcerts = true LIMIT 100'),
            (7, 'SELECT eventname, starttime FROM tickit.event WHERE venueid = 42 AND starttime > GETDATE()'),
            (8, 'VACUUM tickit.sales'),
            (9, 'ANALYZE tickit.listing'),
            (10, 'SELECT u.firstname, u.lastname, u.city, e.eventname, v.venuename, c.catname, s.pricepaid, s.saletime FROM tickit.sales s JOIN tickit.users u ON s.buyerid = u.userid JOIN tickit.event e ON s.eventid = e.eventid JOIN tickit.venue v ON e.venueid = v.venueid JOIN tickit.category c ON e.catid = c.catid WHERE s.pricepaid > 500 ORDER BY s.pricepaid DESC LIMIT 50')
        `});

        var queryStmt = snowflake.createStatement({sqlText: `
            SELECT QUERY_ID, QUERY_TEXT FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.TEMP_SAMPLE_QUERIES ORDER BY QUERY_ID
        `});
        var queryResult = queryStmt.execute();

        while (queryResult.next()) {
            var queryText = queryResult.getColumnValue('QUERY_TEXT');
            var escapedQuery = queryText.replace(/'/g, "''");

            var prompt = 'You are a SQL query classifier. You must respond with exactly one word. Choose from: ETL, REPORTING, AD_HOC, MAINTENANCE. No explanation. No punctuation. One word only. Classify this query: ' + queryText;
            var escapedPrompt = prompt.replace(/'/g, "''");

            var classifyStmt = snowflake.createStatement({sqlText: `
                SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', '${escapedPrompt}') AS CATEGORY
            `});
            var classifyResult = classifyStmt.execute();
            classifyResult.next();

            var rawCategory = classifyResult.getColumnValue('CATEGORY');
            var upperResponse = rawCategory.trim().toUpperCase();

            var category = 'UNKNOWN';
            var confidence = 'LOW';

            if (upperResponse.indexOf('ETL') !== -1) {
                category = 'ETL';
                confidence = 'HIGH';
            } else if (upperResponse.indexOf('REPORTING') !== -1) {
                category = 'REPORTING';
                confidence = 'HIGH';
            } else if (upperResponse.indexOf('AD_HOC') !== -1 || upperResponse.indexOf('ADHOC') !== -1) {
                category = 'AD_HOC';
                confidence = 'HIGH';
            } else if (upperResponse.indexOf('MAINTENANCE') !== -1) {
                category = 'MAINTENANCE';
                confidence = 'HIGH';
            }

            snowflake.execute({sqlText: `
                INSERT INTO MIGRATION_ACCELERATOR_DEV.TRANSLATION.QUERY_CLASSIFICATION_LOG
                    (RUN_ID, QUERY_TEXT, QUERY_CATEGORY, CONFIDENCE)
                VALUES
                    ('${RUN_ID}', '${escapedQuery}', '${category}', '${confidence}')
            `});

            rowsProcessed++;
        }

        snowflake.execute({sqlText: `DROP TABLE IF EXISTS MIGRATION_ACCELERATOR_DEV.TRANSLATION.TEMP_SAMPLE_QUERIES`});

        var durationSecs = Math.round((Date.now() - startTime) / 1000);

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECS)
            VALUES
                ('${RUN_ID}', 'TRANSLATION', 'SP_CLASSIFY_QUERIES', 'SUCCESS', ${rowsProcessed}, ${durationSecs})
        `});

        return 'SUCCESS: Classified ' + rowsProcessed + ' queries';

    } catch (err) {
        var durationSecs = Math.round((Date.now() - startTime) / 1000);
        var errorMsg = err.message.replace(/'/g, "''");

        snowflake.execute({sqlText: `
            INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
                (RUN_ID, PHASE, STEP_NAME, STATUS, DURATION_SECS, ERROR_MESSAGE)
            VALUES
                ('${RUN_ID}', 'TRANSLATION', 'SP_CLASSIFY_QUERIES', 'FAILED', ${durationSecs}, '${errorMsg}')
        `});

        throw err;
    }
$$;


--
-- DELETE FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.QUERY_CLASSIFICATION_LOG;

-- Call the procedure
CALL MIGRATION_ACCELERATOR_DEV.TRANSLATION.SP_CLASSIFY_QUERIES(
    'f71a5e9c-3ec7-4a33-ae84-a0cda464d63a'
);

-- Check classification results
SELECT 
    QUERY_CATEGORY,
    CONFIDENCE,
    LEFT(QUERY_TEXT, 80) AS QUERY_PREVIEW
FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.QUERY_CLASSIFICATION_LOG
ORDER BY QUERY_CATEGORY;

-- Check log
SELECT PHASE, STEP_NAME, STATUS, ROWS_PROCESSED
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
ORDER BY LOGGED_AT;