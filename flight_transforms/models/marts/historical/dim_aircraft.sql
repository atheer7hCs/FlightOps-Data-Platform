{{ config(
    materialized='table',
    schema='HISTORICAL_GOLD'
) }}

WITH ranked_aircraft AS (

    SELECT
        ICAO24,
        REGISTRATION,
        AIRCRAFT_TYPE,
        DESCRIPTION,
        EVENT_DATETIME,

        ROW_NUMBER() OVER (
            PARTITION BY ICAO24
            ORDER BY EVENT_DATETIME DESC
        ) AS rn

    FROM {{ ref('stg_historical_flights') }}

)

SELECT
    ICAO24 AS AIRCRAFT_KEY,
    ICAO24,
    REGISTRATION,
    AIRCRAFT_TYPE,
    DESCRIPTION

FROM ranked_aircraft

WHERE rn = 1