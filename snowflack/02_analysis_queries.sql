-- =========================================================
-- FLIGHT DATA WAREHOUSE — ANALYSIS QUERIES (Snowflake)
-- Run this after 01_warehouse_setup.sql and after dbt has
-- built the dbt_dev schema (stg_flights / fct_flights / dim_airports).
-- =========================================================


-- =========================================================
-- 1. Verify the Bronze Load
-- =========================================================

SELECT COUNT(*) AS total_rows
FROM flight_data.bronze_layer.flights_silver;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT "icao24") AS unique_aircraft,
    COUNT(DISTINCT "snapshot_time") AS snapshots,
    MIN("snapshot_time") AS first_snapshot,
    MAX("snapshot_time") AS last_snapshot
FROM flight_data.bronze_layer.flights_silver;

SHOW TABLES IN SCHEMA flight_data.bronze_layer;

DESC TABLE flight_data.bronze_layer.flights_silver;


-- =========================================================
-- 2. dbt Star Schema — Exploration
-- =========================================================

SELECT * FROM flight_data.dbt_dev.stg_flights LIMIT 10;

SELECT COUNT(*) FROM flight_data.dbt_dev.fct_flights;

DESC TABLE flight_data.dbt_dev.fct_flights;


-- =========================================================
-- 3. dbt Star Schema — Analytics
-- =========================================================

-- Try an actual analytical question we can now answer easily
-- thanks to the star schema
SELECT
    a.airport_code,
    COUNT(*) AS num_departures,
    AVG(f.flight_duration_minutes) AS avg_duration_min
FROM flight_data.dbt_dev.fct_flights f
JOIN flight_data.dbt_dev.dim_airports a
    ON f.departure_airport_key = a.airport_key
GROUP BY a.airport_code
ORDER BY num_departures DESC
LIMIT 10;

SELECT
    COUNT(*) AS total_flights,
    COUNT(DISTINCT aircraft_id) AS unique_aircraft,
    COUNT(DISTINCT departure_airport_key) AS departure_airports,
    COUNT(DISTINCT arrival_airport_key) AS arrival_airports,
    ROUND(AVG(flight_duration_minutes), 2) AS avg_duration_minutes,
    MAX(flight_duration_minutes) AS max_duration_minutes
FROM flight_data.dbt_dev.fct_flights;

SELECT
    MIN(departure_time) AS first_flight,
    MAX(departure_time) AS last_flight
FROM flight_data.dbt_dev.fct_flights;


-- =========================================================
-- 4. Data Quality Checks
-- =========================================================

SELECT
    COUNT(*) AS total_rows,
    COUNT(departure_airport_key) AS flights_with_departure,
    COUNT(arrival_airport_key) AS flights_with_arrival,
    COUNT(departure_date_key) AS flights_with_date
FROM flight_data.dbt_dev.fct_flights;

SELECT
    COUNT(*) AS total_rows,
    COUNT_IF(departure_airport_key IS NULL) AS missing_departure_key,
    COUNT_IF(arrival_airport_key IS NULL) AS missing_arrival_key
FROM flight_data.dbt_dev.fct_flights;

SELECT
    COUNT(*) AS total_rows,
    COUNT_IF(departure_airport IS NULL) AS missing_departure_airport,
    COUNT_IF(arrival_airport IS NULL) AS missing_arrival_airport
FROM flight_data.dbt_dev.stg_flights;