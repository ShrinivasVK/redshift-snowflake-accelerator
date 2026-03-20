-- ==========================================
-- Step 1 — Show the Control Layer (1 minute)

SELECT 
    * 
FROM 
    MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY;

-- ==================================
-- Show Translation Config (1 minute)

SELECT 
    CONFIG_TYPE, 
    SOURCE_VALUE, 
    TARGET_VALUE, 
    NOTES
FROM 
    MIGRATION_ACCELERATOR_DEV.CONTROL.TRANSLATION_CONFIG
WHERE CONFIG_ID IN (
    SELECT MIN(CONFIG_ID) FROM MIGRATION_ACCELERATOR_DEV.CONTROL.TRANSLATION_CONFIG
    GROUP BY CONFIG_TYPE, SOURCE_VALUE
);


-- ===========================================================
-- 3. Show DDL Translation

SELECT
    SOURCE_TABLE, 
    ORIGINAL_DDL,
    TRANSLATED_DDL
FROM 
    MIGRATION_ACCELERATOR_DEV.TRANSLATION.DDL_TRANSLATION_LOG
WHERE 
    SOURCE_TABLE = 'users';

-- CREATE TABLE tickit.users (
--     userid INTEGER NOT NULL ENCODE az64,
--     username CHAR(8) ENCODE lzo,
--     firstname VARCHAR(30) ENCODE lzo,
--     lastname VARCHAR(30) ENCODE lzo,
--     city VARCHAR(30) ENCODE lzo,
--     state CHAR(2) ENCODE lzo,
--     email VARCHAR(100) ENCODE lzo,
--     phone CHAR(14) ENCODE lzo,
--     likesports BOOLEAN ENCODE raw,
--     liketheatre BOOLEAN ENCODE raw,
--     likeconcerts BOOLEAN ENCODE raw,
--     likejazz BOOLEAN ENCODE raw,
--     likeclassical BOOLEAN ENCODE raw,
--     likeopera BOOLEAN ENCODE raw,
--     likerock BOOLEAN ENCODE raw,
--     likevegas BOOLEAN ENCODE raw,
--     likebroadway BOOLEAN ENCODE raw,
--     likemusicals BOOLEAN ENCODE raw,
--     PRIMARY KEY (userid)
-- )
-- DISTSTYLE KEY
-- DISTKEY (userid)
-- SORTKEY (userid);

-- CREATE OR REPLACE TABLE users (
--     userid INTEGER NOT NULL ,
--     username CHAR(8) ,
--     firstname VARCHAR(30) ,
--     lastname VARCHAR(30) ,
--     city VARCHAR(30) ,
--     state CHAR(2) ,
--     email VARCHAR(100) ,
--     phone CHAR(14) ,
--     likesports BOOLEAN ,
--     liketheatre BOOLEAN ,
--     likeconcerts BOOLEAN ,
--     likejazz BOOLEAN ,
--     likeclassical BOOLEAN ,
--     likeopera BOOLEAN ,
--     likerock BOOLEAN ,
--     likevegas BOOLEAN ,
--     likebroadway BOOLEAN ,
--     likemusicals BOOLEAN ,
--     PRIMARY KEY (userid)
-- );

-- =============================================
-- Step 4 — Show Data Loading Results (1 minute)

SELECT 
    'CATEGORY' AS TBL, 
    COUNT(*) AS CNT 
FROM 
    MIGRATION_ACCELERATOR_DEV.TARGET.CATEGORY
    UNION ALL 
        SELECT 'DATE', COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.DATE
    UNION 
        ALL SELECT 'EVENT', COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.EVENT
    UNION 
        ALL SELECT 'LISTING', COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.LISTING
    UNION 
        ALL SELECT 'SALES', COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.SALES
    UNION 
        ALL SELECT 'USERS', COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.USERS
    UNION 
        ALL SELECT 'VENUE', COUNT(*) FROM MIGRATION_ACCELERATOR_DEV.TARGET.VENUE
ORDER BY 
    TBL;


-- ===========================================
-- Step 5 — Show Validation Results (1 minute)

SELECT 
    CHECK_NAME, 
    SOURCE_VALUE, 
    TARGET_VALUE, 
    VARIANCE_PCT, 
    STATUS
FROM 
    MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
WHERE 
    CHECK_NAME LIKE 'ROW_COUNT%'
ORDER BY 
    CHECK_NAME;


-- ==========================================
-- Step 6 — Show the Audit Trail (30 seconds)

SELECT
    PHASE, 
    STEP_NAME, 
    STATUS, 
    ROWS_PROCESSED, 
    DURATION_SECS
FROM 
    MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
ORDER BY 
    LOGGED_AT;