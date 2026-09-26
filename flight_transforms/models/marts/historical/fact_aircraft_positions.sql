{{ config(
    materialized='table',
    schema='HISTORICAL_GOLD'
) }}

SELECT

    SHA2(
        CONCAT(
            ICAO24,
            '|',
            EVENT_TIMESTAMP
        ),
        256
    ) AS POSITION_KEY,

    ICAO24 AS AIRCRAFT_KEY,

    TO_NUMBER(
        TO_CHAR(EVENT_DATE, 'YYYYMMDD')
    ) AS DATE_KEY,

    EVENT_TIMESTAMP,
    EVENT_DATETIME,
    EVENT_DATE,
    EVENT_HOUR,

    LATITUDE,
    LONGITUDE,

    ALTITUDE_FT,
    ALTITUDE_BAND,

    ON_GROUND,
    ON_GROUND_FLAG,
    ON_GROUND_STATUS,

    CALLSIGN,
    SQUAWK,

    SOURCE

FROM {{ ref('stg_historical_flights') }}