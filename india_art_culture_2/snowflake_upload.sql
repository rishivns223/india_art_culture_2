-- Create table for gender-wise tourism data
CREATE TABLE IF NOT EXISTS GENDER_TOURISM (
    YEAR INTEGER,
    TOTAL_ARRIVALS INTEGER,
    MALE_PCT FLOAT,
    FEMALE_PCT FLOAT,
    NOT_REPORTED_PCT FLOAT,
    MALE_COUNT INTEGER,
    FEMALE_COUNT INTEGER,
    NOT_REPORTED_COUNT INTEGER
);

-- Create table for geological heritage sites
CREATE TABLE IF NOT EXISTS GEOLOGICAL_SITES (
    SL_NO INTEGER,
    STATE VARCHAR(100),
    SITE_NAME VARCHAR(500),
    SITE_TYPE VARCHAR(100)
);

-- Create table for monuments data
CREATE TABLE IF NOT EXISTS MONUMENTS (
    SL_NO INTEGER,
    STATE VARCHAR(100),
    MONUMENTS INTEGER
);

-- Create table for tourism statistics (2016-2018)
CREATE TABLE IF NOT EXISTS TOURISM_STATS_2016_2018 (
    STATE VARCHAR(100),
    YEAR INTEGER,
    DOMESTIC_VISITORS BIGINT,
    FOREIGN_VISITORS INTEGER
);

-- Create table for tourism statistics (2019-2021)
CREATE TABLE IF NOT EXISTS TOURISM_STATS_2019_2021 (
    SL_NO INTEGER,
    STATE VARCHAR(100),
    YEAR INTEGER,
    DOMESTIC_VISITORS BIGINT,
    FOREIGN_VISITORS INTEGER
);

-- After creating tables, use the Snowflake web interface to:
-- 1. Create a file format for CSV:
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- 2. Create stages for each data file:
CREATE OR REPLACE STAGE gender_tourism_stage
    FILE_FORMAT = csv_format;

CREATE OR REPLACE STAGE geological_sites_stage
    FILE_FORMAT = csv_format;

CREATE OR REPLACE STAGE monuments_stage
    FILE_FORMAT = csv_format;

CREATE OR REPLACE STAGE tourism_2016_2018_stage
    FILE_FORMAT = csv_format;

CREATE OR REPLACE STAGE tourism_2019_2021_stage
    FILE_FORMAT = csv_format;

-- 3. Upload data files using PUT command:
-- PUT file:///Users/rdhardubey/india_art_culture/data/raw/India-Tourism-Statistics-2019-Table-2.6.1.csv @gender_tourism_stage;
-- PUT file:///Users/rdhardubey/india_art_culture/data/raw/rs_session-238_AU1380_1.1.csv @geological_sites_stage;
-- PUT file:///Users/rdhardubey/india_art_culture/data/raw/session_244_AU1787_1.1.csv @monuments_stage;
-- PUT file:///Users/rdhardubey/india_art_culture/data/raw/RS-Session-251-AU308-Annexure-I.csv @tourism_2016_2018_stage;
-- PUT file:///Users/rdhardubey/india_art_culture/data/raw/RS_Session_259_AU_1898_B_and_C.csv @tourism_2019_2021_stage;

-- 4. Copy data from staged files:
COPY INTO GENDER_TOURISM (YEAR, TOTAL_ARRIVALS, MALE_PCT, FEMALE_PCT, NOT_REPORTED_PCT)
FROM @gender_tourism_stage
FILE_FORMAT = csv_format;

COPY INTO GEOLOGICAL_SITES (SL_NO, STATE, SITE_NAME)
FROM @geological_sites_stage
FILE_FORMAT = csv_format;

COPY INTO MONUMENTS (SL_NO, STATE, MONUMENTS)
FROM @monuments_stage
FILE_FORMAT = csv_format;

COPY INTO TOURISM_STATS_2016_2018 (STATE, YEAR, DOMESTIC_VISITORS, FOREIGN_VISITORS)
FROM (
    SELECT 
        $1,  -- State
        2016,  -- Year
        $2,  -- Domestic visitors 2016
        $3   -- Foreign visitors 2016
    FROM @tourism_2016_2018_stage
)
FILE_FORMAT = csv_format;

COPY INTO TOURISM_STATS_2016_2018 (STATE, YEAR, DOMESTIC_VISITORS, FOREIGN_VISITORS)
FROM (
    SELECT 
        $1,  -- State
        2017,  -- Year
        $4,  -- Domestic visitors 2017
        $5   -- Foreign visitors 2017
    FROM @tourism_2016_2018_stage
)
FILE_FORMAT = csv_format;

COPY INTO TOURISM_STATS_2016_2018 (STATE, YEAR, DOMESTIC_VISITORS, FOREIGN_VISITORS)
FROM (
    SELECT 
        $1,  -- State
        2018,  -- Year
        $6,  -- Domestic visitors 2018
        $7   -- Foreign visitors 2018
    FROM @tourism_2016_2018_stage
)
FILE_FORMAT = csv_format;

COPY INTO TOURISM_STATS_2019_2021 (SL_NO, STATE, YEAR, DOMESTIC_VISITORS, FOREIGN_VISITORS)
FROM (
    SELECT 
        $1,  -- Sl. No.
        $2,  -- State
        2019,  -- Year
        $3,  -- Domestic visitors 2019
        $4   -- Foreign visitors 2019
    FROM @tourism_2019_2021_stage
)
FILE_FORMAT = csv_format;

COPY INTO TOURISM_STATS_2019_2021 (SL_NO, STATE, YEAR, DOMESTIC_VISITORS, FOREIGN_VISITORS)
FROM (
    SELECT 
        $1,  -- Sl. No.
        $2,  -- State
        2020,  -- Year
        $5,  -- Domestic visitors 2020
        $6   -- Foreign visitors 2020
    FROM @tourism_2019_2021_stage
)
FILE_FORMAT = csv_format;

COPY INTO TOURISM_STATS_2019_2021 (SL_NO, STATE, YEAR, DOMESTIC_VISITORS, FOREIGN_VISITORS)
FROM (
    SELECT 
        $1,  -- Sl. No.
        $2,  -- State
        2021,  -- Year
        $7,  -- Domestic visitors 2021
        $8   -- Foreign visitors 2021
    FROM @tourism_2019_2021_stage
)
FILE_FORMAT = csv_format;

-- 5. Create a view to combine all tourism statistics
CREATE OR REPLACE VIEW TOURISM_STATS AS
SELECT STATE, YEAR, DOMESTIC_VISITORS, FOREIGN_VISITORS
FROM TOURISM_STATS_2016_2018
UNION ALL
SELECT STATE, YEAR, DOMESTIC_VISITORS, FOREIGN_VISITORS
FROM TOURISM_STATS_2019_2021
WHERE STATE != 'Total';  -- Exclude total row

-- 6. Update site types in geological sites
UPDATE GEOLOGICAL_SITES
SET SITE_TYPE = CASE
    WHEN LOWER(SITE_NAME) LIKE '%fossil%' THEN 'Fossil Site'
    WHEN LOWER(SITE_NAME) LIKE ANY ('%lava%', '%volcanic%', '%igneous%') THEN 'Volcanic/Igneous Formation'
    WHEN LOWER(SITE_NAME) LIKE ANY ('%fault%', '%unconformity%') THEN 'Geological Structure'
    WHEN LOWER(SITE_NAME) LIKE ANY ('%lake%', '%cliff%', '%island%') THEN 'Natural Formation'
    ELSE 'Other Geological Site'
END; 