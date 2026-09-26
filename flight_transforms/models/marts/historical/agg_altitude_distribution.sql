{{ config(
    materialized='table',
    schema='HISTORICAL_GOLD'
) }}

SELECT
    EVENT_DATE,

    TO_NUMBER(
        TO_CHAR(EVENT_DATE, 'YYYYMMDD')
    ) AS DATE_KEY,

    ALTITUDE_BAND,

    COUNT(*) AS POSITION_COUNT,

    COUNT(DISTINCT AIRCRAFT_KEY) AS UNIQUE_AIRCRAFT,

    ROUND(
        AVG(ALTITUDE_FT),
        2
    ) AS AVG_ALTITUDE_FT,

    ROUND(
        MIN(ALTITUDE_FT),
        2
    ) AS MIN_ALTITUDE_FT,

    ROUND(
        MAX(ALTITUDE_FT),
        2
    ) AS MAX_ALTITUDE_FT

FROM {{ ref('fact_aircraft_positions') }}

GROUP BY
    EVENT_DATE,
    ALTITUDE_BAND