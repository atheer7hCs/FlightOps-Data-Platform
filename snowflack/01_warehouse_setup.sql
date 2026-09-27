-- =========================================================
-- FLIGHT DATA WAREHOUSE — SETUP (Snowflake)
-- Run this file once to provision the environment.
-- =========================================================


-- =========================================================
-- 1. Database, Schema, and Warehouse
-- =========================================================

CREATE DATABASE flight_data;

CREATE SCHEMA flight_data.bronze_layer;

CREATE WAREHOUSE flight_wh
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;


-- =========================================================
-- 2. Storage Integration (connects Snowflake to the S3 bucket)
-- This MUST be created before the stage, since the stage
-- references this integration by name.
-- =========================================================

DROP STORAGE INTEGRATION IF EXISTS s3_flight_integration;

CREATE STORAGE INTEGRATION s3_flight_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '*********************'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://flight-data-lake-2026/processed/states/'
  );

-- Use this output to copy STORAGE_AWS_IAM_USER_ARN and
-- STORAGE_AWS_EXTERNAL_ID into the AWS IAM role's trust policy
DESC INTEGRATION s3_flight_integration;


-- =========================================================
-- 3. External Stage (points to the S3 folder)
-- =========================================================

DROP STAGE IF EXISTS flight_data.bronze_layer.processed_stage;

CREATE STAGE flight_data.bronze_layer.processed_stage
  URL = 's3://flight-data-lake-2026/processed/states/'
  STORAGE_INTEGRATION = s3_flight_integration;

LIST @flight_data.bronze_layer.processed_stage;

DESC STAGE flight_data.bronze_layer.processed_stage;


-- =========================================================
-- 4. File Format
-- =========================================================

CREATE OR REPLACE FILE FORMAT flight_data.bronze_layer.parquet_format
  TYPE = PARQUET;


-- =========================================================
-- 5. Create Bronze Table (schema inferred from the parquet files)
-- =========================================================

CREATE OR REPLACE TABLE flight_data.bronze_layer.flights_silver
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION => '@flight_data.bronze_layer.processed_stage',
        FILE_FORMAT => 'flight_data.bronze_layer.parquet_format'
      )
    )
  );


-- =========================================================
-- 6. Load Data into the Bronze Table
-- =========================================================

COPY INTO flight_data.bronze_layer.flights_silver
  FROM @flight_data.bronze_layer.processed_stage
  FILE_FORMAT = (FORMAT_NAME = 'flight_data.bronze_layer.parquet_format')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Setup complete. dbt then builds stg_flights / fct_flights /
-- dim_airports in the dbt_dev schema on top of this bronze table.
-- Continue with 02_analysis_queries.sql for exploration and checks.