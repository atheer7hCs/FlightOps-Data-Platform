-- =========================================================
-- HISTORICAL FLIGHT DATA — SETUP (Snowflake)
-- Bronze → Silver → Gold pipeline for historical flights,
-- separate from the live "states" pipeline (bronze_layer).
-- =========================================================

USE DATABASE FLIGHT_DATA;


-- =========================================================
-- 1. Schemas
-- =========================================================

CREATE SCHEMA IF NOT EXISTS HISTORICAL_BRONZE;
CREATE SCHEMA IF NOT EXISTS HISTORICAL_SILVER;
CREATE SCHEMA IF NOT EXISTS HISTORICAL_GOLD;

SHOW SCHEMAS IN DATABASE FLIGHT_DATA;


-- =========================================================
-- 2. Storage Integration — allow the new historical S3 path
-- =========================================================

SHOW STORAGE INTEGRATIONS;

DESC INTEGRATION S3_FLIGHT_INTEGRATION;

ALTER STORAGE INTEGRATION S3_FLIGHT_INTEGRATION
SET STORAGE_ALLOWED_LOCATIONS = (
    's3://flight-data-lake-2026/processed/states/',
    's3://flight-data-lake-2026/processed/flights/cleaned/'
);

-- Confirm the new location was added
DESC INTEGRATION S3_FLIGHT_INTEGRATION;


-- =========================================================
-- 3. File Format
-- =========================================================

CREATE FILE FORMAT IF NOT EXISTS FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_PARQUET_FORMAT
TYPE = PARQUET;

SHOW FILE FORMATS IN SCHEMA FLIGHT_DATA.HISTORICAL_BRONZE;


-- =========================================================
-- 4. External Stage
-- =========================================================

CREATE STAGE IF NOT EXISTS FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_S3_STAGE
URL = 's3://flight-data-lake-2026/processed/flights/cleaned/'
STORAGE_INTEGRATION = S3_FLIGHT_INTEGRATION
FILE_FORMAT = FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_PARQUET_FORMAT;

SHOW STAGES IN SCHEMA FLIGHT_DATA.HISTORICAL_BRONZE;

-- Test the S3 connection
LIST @FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_S3_STAGE;


-- =========================================================
-- 5. Infer Schema from the Parquet Files
-- =========================================================

SELECT
    COLUMN_NAME,
    TYPE,
    NULLABLE
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_S3_STAGE',
        FILE_FORMAT => 'FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_PARQUET_FORMAT'
    )
)
ORDER BY ORDER_ID;


-- =========================================================
-- 6. Create the Bronze Table
-- The table was actually created with the auto-inferred
-- template below (column names came in lowercase and quoted,
-- which is why section 8 renames them to uppercase).
-- The explicit column list is kept here as a documented,
-- equivalent alternative — it was not the one executed.
-- =========================================================

CREATE TABLE IF NOT EXISTS FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_S3_STAGE',
            FILE_FORMAT => 'FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_PARQUET_FORMAT'
        )
    )
);

-- Alternative (not executed): explicit column definitions
-- CREATE TABLE IF NOT EXISTS FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS (
--     ICAO24          VARCHAR,
--     REGISTRATION    VARCHAR,
--     AIRCRAFT_TYPE   VARCHAR,
--     DESCRIPTION     VARCHAR,
--     CALLSIGN        VARCHAR,
--     SQUAWK          FLOAT,
--     LATITUDE        FLOAT,
--     LONGITUDE       FLOAT,
--     ALTITUDE_FT     FLOAT,
--     ON_GROUND       NUMBER(38,0),
--     EVENT_TIMESTAMP NUMBER(38,0),
--     EVENT_DATETIME  TIMESTAMP_NTZ,
--     EVENT_HOUR      NUMBER(38,0),
--     ALTITUDE_BAND   VARCHAR,
--     SOURCE          VARCHAR
-- );

DESC TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS;


-- =========================================================
-- 7. Load Data
-- =========================================================

COPY INTO FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS
FROM @FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_S3_STAGE
FILE_FORMAT = (
    FORMAT_NAME = 'FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_PARQUET_FORMAT'
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT COUNT(*) AS TOTAL_ROWS
FROM FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS;


-- =========================================================
-- 8. Standardize Column Casing
-- The inferred schema created lowercase, quoted column names
-- (e.g. "icao24"). This renames them to plain uppercase
-- identifiers, consistent with the rest of the warehouse.
-- =========================================================

ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "icao24" TO ICAO24;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "registration" TO REGISTRATION;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "aircraft_type" TO AIRCRAFT_TYPE;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "description" TO DESCRIPTION;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "callsign" TO CALLSIGN;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "squawk" TO SQUAWK;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "latitude" TO LATITUDE;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "longitude" TO LONGITUDE;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "altitude_ft" TO ALTITUDE_FT;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "on_ground" TO ON_GROUND;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "event_timestamp" TO EVENT_TIMESTAMP;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "event_datetime" TO EVENT_DATETIME;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "event_hour" TO EVENT_HOUR;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "altitude_band" TO ALTITUDE_BAND;
ALTER TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS RENAME COLUMN "source" TO SOURCE;

DESCRIBE TABLE FLIGHT_DATA.HISTORICAL_BRONZE.HISTORICAL_FLIGHTS;

-- Setup complete. dbt then builds STG_HISTORICAL_FLIGHTS in
-- dbt_dev_HISTORICAL_SILVER, and FACT_AIRCRAFT_POSITIONS /
-- DIM_DATES in DBT_DEV (gold layer), on top of this bronze table.
-- Continue with 04_historical_analysis_queries.sql.