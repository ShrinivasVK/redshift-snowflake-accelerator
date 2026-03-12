USE ROLE SYSADMIN;

USE DATABASE MIGRATION_ACCELERATOR_DEV;

CREATE SCHEMA IF NOT EXISTS REDSHIFT_MIRROR;

USE SCHEMA REDSHIFT_MIRROR;

CREATE TABLE IF NOT EXISTS SOURCE_TABLE_INVENTORY (
    SOURCE_SCHEMA       VARCHAR(128),
    SOURCE_TABLE        VARCHAR(128),
    ROW_COUNT_EST       NUMBER,
    SIZE_BYTES_EST      NUMBER,
    HAS_DISTKEY         BOOLEAN,
    HAS_SORTKEY         BOOLEAN,
    HAS_STORED_PROCS    BOOLEAN,
    REGISTERED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SOURCE_DDL_STORE (
    SOURCE_SCHEMA       VARCHAR(128),
    SOURCE_TABLE        VARCHAR(128),
    ORIGINAL_DDL        VARCHAR(16777216),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO SOURCE_TABLE_INVENTORY (SOURCE_SCHEMA, SOURCE_TABLE, ROW_COUNT_EST, SIZE_BYTES_EST, HAS_DISTKEY, HAS_SORTKEY, HAS_STORED_PROCS)
VALUES
    ('tickit', 'users',    49990,   3200000,  TRUE,  TRUE,  FALSE),
    ('tickit', 'venue',      202,     15000,  TRUE,  TRUE,  FALSE),
    ('tickit', 'category',    11,      1000,  FALSE, TRUE,  FALSE),
    ('tickit', 'date',       365,     25000,  FALSE, TRUE,  FALSE),
    ('tickit', 'event',     8798,    560000,  TRUE,  TRUE,  FALSE),
    ('tickit', 'listing',  192497,  12500000, TRUE,  TRUE,  FALSE),
    ('tickit', 'sales',    172456,  11000000, TRUE,  TRUE,  FALSE);

UPDATE MIGRATION_ACCELERATOR_DEV.REDSHIFT_MIRROR.SOURCE_TABLE_INVENTORY
SET ROW_COUNT_EST = 187
WHERE SOURCE_TABLE = 'venue';

-- Verify
SELECT SOURCE_TABLE, ROW_COUNT_EST
FROM MIGRATION_ACCELERATOR_DEV.REDSHIFT_MIRROR.SOURCE_TABLE_INVENTORY
WHERE SOURCE_TABLE = 'venue';

INSERT INTO SOURCE_DDL_STORE (SOURCE_SCHEMA, SOURCE_TABLE, ORIGINAL_DDL)
VALUES
    ('tickit', 'users', 
'CREATE TABLE tickit.users (
    userid INTEGER NOT NULL ENCODE az64,
    username CHAR(8) ENCODE lzo,
    firstname VARCHAR(30) ENCODE lzo,
    lastname VARCHAR(30) ENCODE lzo,
    city VARCHAR(30) ENCODE lzo,
    state CHAR(2) ENCODE lzo,
    email VARCHAR(100) ENCODE lzo,
    phone CHAR(14) ENCODE lzo,
    likesports BOOLEAN ENCODE raw,
    liketheatre BOOLEAN ENCODE raw,
    likeconcerts BOOLEAN ENCODE raw,
    likejazz BOOLEAN ENCODE raw,
    likeclassical BOOLEAN ENCODE raw,
    likeopera BOOLEAN ENCODE raw,
    likerock BOOLEAN ENCODE raw,
    likevegas BOOLEAN ENCODE raw,
    likebroadway BOOLEAN ENCODE raw,
    likemusicals BOOLEAN ENCODE raw,
    PRIMARY KEY (userid)
)
DISTSTYLE KEY
DISTKEY (userid)
SORTKEY (userid);'),

    ('tickit', 'venue',
'CREATE TABLE tickit.venue (
    venueid SMALLINT NOT NULL ENCODE az64,
    venuename VARCHAR(100) ENCODE lzo,
    venuecity VARCHAR(30) ENCODE lzo,
    venuestate CHAR(2) ENCODE lzo,
    venueseats INTEGER ENCODE az64,
    PRIMARY KEY (venueid)
)
DISTSTYLE KEY
DISTKEY (venueid)
SORTKEY (venuecity, venuestate);'),

    ('tickit', 'category',
'CREATE TABLE tickit.category (
    catid SMALLINT NOT NULL ENCODE az64,
    catgroup VARCHAR(10) ENCODE lzo,
    catname VARCHAR(10) ENCODE lzo,
    catdesc VARCHAR(50) ENCODE lzo,
    PRIMARY KEY (catid)
)
DISTSTYLE ALL
SORTKEY (catid);'),

    ('tickit', 'date',
'CREATE TABLE tickit.date (
    dateid SMALLINT NOT NULL ENCODE az64,
    caldate DATE NOT NULL ENCODE az64,
    day CHAR(3) NOT NULL ENCODE lzo,
    week SMALLINT NOT NULL ENCODE az64,
    month CHAR(5) NOT NULL ENCODE lzo,
    qtr CHAR(5) NOT NULL ENCODE lzo,
    year SMALLINT NOT NULL ENCODE az64,
    holiday BOOLEAN DEFAULT FALSE ENCODE raw,
    PRIMARY KEY (dateid)
)
DISTSTYLE ALL
SORTKEY (dateid);'),

    ('tickit', 'event',
'CREATE TABLE tickit.event (
    eventid INTEGER NOT NULL ENCODE az64,
    venueid SMALLINT NOT NULL ENCODE az64,
    catid SMALLINT NOT NULL ENCODE az64,
    dateid SMALLINT NOT NULL ENCODE az64,
    eventname VARCHAR(200) ENCODE lzo,
    starttime TIMESTAMP ENCODE az64,
    PRIMARY KEY (eventid)
)
DISTSTYLE KEY
DISTKEY (eventid)
SORTKEY (dateid, eventid);'),

    ('tickit', 'listing',
'CREATE TABLE tickit.listing (
    listid INTEGER NOT NULL ENCODE az64,
    sellerid INTEGER NOT NULL ENCODE az64,
    eventid INTEGER NOT NULL ENCODE az64,
    dateid SMALLINT NOT NULL ENCODE az64,
    numtickets SMALLINT NOT NULL ENCODE az64,
    priceperticket DECIMAL(8,2) ENCODE az64,
    totalprice DECIMAL(8,2) ENCODE az64,
    listtime TIMESTAMP ENCODE az64,
    PRIMARY KEY (listid)
)
DISTSTYLE KEY
DISTKEY (listid)
SORTKEY (dateid, listid);'),

    ('tickit', 'sales',
'CREATE TABLE tickit.sales (
    salesid INTEGER NOT NULL ENCODE az64,
    listid INTEGER NOT NULL ENCODE az64,
    sellerid INTEGER NOT NULL ENCODE az64,
    buyerid INTEGER NOT NULL ENCODE az64,
    eventid INTEGER NOT NULL ENCODE az64,
    dateid SMALLINT NOT NULL ENCODE az64,
    qtysold SMALLINT NOT NULL ENCODE az64,
    pricepaid DECIMAL(8,2) ENCODE az64,
    commission DECIMAL(8,2) ENCODE az64,
    saletime TIMESTAMP ENCODE az64,
    PRIMARY KEY (salesid)
)
DISTSTYLE KEY
DISTKEY (salesid)
SORTKEY (dateid, salesid);');


-- USE DATABASE MIGRATION_ACCELERATOR_DEV;
-- USE SCHEMA REDSHIFT_MIRROR;

-- SELECT SOURCE_TABLE, ROW_COUNT_EST, HAS_DISTKEY, HAS_SORTKEY
-- FROM SOURCE_TABLE_INVENTORY
-- ORDER BY SOURCE_TABLE;