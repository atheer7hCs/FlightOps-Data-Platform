-- =========================================================
-- ADVANCED ANALYTICS — SHOWCASE QUERIES (Snowflake)
-- Uses window functions, CTEs, and ranking to surface
-- patterns worth showing off in a portfolio / screenshots.
-- Built on the Historical Silver layer (STG_HISTORICAL_FLIGHTS)
-- unless noted otherwise.
-- =========================================================


-- =========================================================
-- 1. Most Active Aircraft — Ranked
-- Top 15 aircraft by number of tracked positions.
-- =========================================================

SELECT
    RANK() OVER (ORDER BY COUNT(*) DESC) AS RANK,
    ICAO24,
    ANY_VALUE(REGISTRATION) AS REGISTRATION,
    ANY_VALUE(CALLSIGN) AS CALLSIGN,
    COUNT(*) AS TOTAL_POSITIONS,
    ROUND(AVG(ALTITUDE_FT), 0) AS AVG_ALTITUDE_FT
FROM FLIGHT_DATA.dbt_dev_HISTORICAL_SILVER.STG_HISTORICAL_FLIGHTS
GROUP BY ICAO24
ORDER BY TOTAL_POSITIONS DESC
LIMIT 15;


-- =========================================================
-- 2. Hourly Traffic Pattern
-- Position count per hour of day, with % share of total.
-- Good for a bar chart showing peak traffic hours.
-- =========================================================

SELECT
    EVENT_HOUR,
    COUNT(*) AS TOTAL_POSITIONS,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS PCT_OF_TOTAL
FROM FLIGHT_DATA.dbt_dev_HISTORICAL_SILVER.STG_HISTORICAL_FLIGHTS
GROUP BY EVENT_HOUR
ORDER BY EVENT_HOUR;


-- =========================================================
-- 3. Day-over-Day Trend
-- Daily row counts with previous-day comparison and % change.
-- Great for a line chart with growth annotations.
-- =========================================================

WITH DAILY AS (
    SELECT
        EVENT_DATE,
        COUNT(*) AS TOTAL_ROWS
    FROM FLIGHT_DATA.dbt_dev_HISTORICAL_SILVER.STG_HISTORICAL_FLIGHTS
    GROUP BY EVENT_DATE
)
SELECT
    EVENT_DATE,
    TOTAL_ROWS,
    LAG(TOTAL_ROWS) OVER (ORDER BY EVENT_DATE) AS PREV_DAY_ROWS,
    ROUND(
        100.0 * (TOTAL_ROWS - LAG(TOTAL_ROWS) OVER (ORDER BY EVENT_DATE))
        / NULLIF(LAG(TOTAL_ROWS) OVER (ORDER BY EVENT_DATE), 0), 2
    ) AS PCT_CHANGE
FROM DAILY
ORDER BY EVENT_DATE;


-- =========================================================
-- 4. Altitude Band Breakdown with Cumulative Share
-- Shows the split between low/mid/high altitude traffic,
-- with a running (cumulative) percentage — a classic
-- Pareto-style view.
-- =========================================================

SELECT
    ALTITUDE_BAND,
    COUNT(*) AS TOTAL_ROWS,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS PCT_OF_TOTAL,
    ROUND(
        100.0 * SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC)
        / SUM(COUNT(*)) OVER (), 2
    ) AS CUMULATIVE_PCT
FROM FLIGHT_DATA.dbt_dev_HISTORICAL_SILVER.STG_HISTORICAL_FLIGHTS
GROUP BY ALTITUDE_BAND
ORDER BY TOTAL_ROWS DESC;


-- =========================================================
-- 5. Peak Concurrent Aircraft in the Air
-- Distinct airborne aircraft per date/hour — the busiest
-- hour for the sky, not just for row counts.
-- =========================================================

SELECT
    EVENT_DATE,
    EVENT_HOUR,
    COUNT(DISTINCT ICAO24) AS AIRCRAFT_IN_AIR
FROM FLIGHT_DATA.dbt_dev_HISTORICAL_SILVER.STG_HISTORICAL_FLIGHTS
WHERE ON_GROUND_FLAG = FALSE
GROUP BY EVENT_DATE, EVENT_HOUR
ORDER BY AIRCRAFT_IN_AIR DESC
LIMIT 10;


-- =========================================================
-- 6. Flight Session Detection (Gap Analysis)
-- For each aircraft, flags a "new flight" whenever the gap
-- since its last tracked position exceeds 30 minutes.
-- Demonstrates LAG() + DATEDIFF for session/event detection.
-- =========================================================

WITH ORDERED AS (
    SELECT
        ICAO24,
        EVENT_DATETIME,
        LAG(EVENT_DATETIME) OVER (
            PARTITION BY ICAO24 ORDER BY EVENT_DATETIME
        ) AS PREV_EVENT_TIME
    FROM FLIGHT_DATA.dbt_dev_HISTORICAL_SILVER.STG_HISTORICAL_FLIGHTS
)
SELECT
    ICAO24,
    EVENT_DATETIME,
    PREV_EVENT_TIME,
    DATEDIFF('minute', PREV_EVENT_TIME, EVENT_DATETIME) AS MINUTES_SINCE_LAST
FROM ORDERED
WHERE DATEDIFF('minute', PREV_EVENT_TIME, EVENT_DATETIME) > 30
ORDER BY MINUTES_SINCE_LAST DESC
LIMIT 20;


-- =========================================================
-- 7. Airport Traffic Ranking (Gold Layer — Live Data)
-- Uses the star schema (fct_flights / dim_airports) instead
-- of the historical tables. Ranked departures with % share.
-- =========================================================

WITH AIRPORT_TRAFFIC AS (
    SELECT
        a.airport_code,
        COUNT(*) AS num_departures
    FROM flight_data.dbt_dev.fct_flights f
    JOIN flight_data.dbt_dev.dim_airports a
        ON f.departure_airport_key = a.airport_key
    GROUP BY a.airport_code
)
SELECT
    airport_code,
    num_departures,
    RANK() OVER (ORDER BY num_departures DESC) AS RANK,
    ROUND(100.0 * num_departures / SUM(num_departures) OVER (), 2) AS PCT_OF_TOTAL
FROM AIRPORT_TRAFFIC
ORDER BY num_departures DESC
LIMIT 10;
