{{ config(
    materialized='table',
    schema='HISTORICAL_GOLD'
) }}

SELECT
    HOUR AS TIME_KEY,

    HOUR AS HOUR_NUMBER,

    CASE
        WHEN HOUR = 0 THEN '12 AM'
        WHEN HOUR < 12 THEN HOUR || ' AM'
        WHEN HOUR = 12 THEN '12 PM'
        ELSE (HOUR - 12) || ' PM'
    END AS HOUR_LABEL,

    CASE
        WHEN HOUR BETWEEN 6 AND 11 THEN 'Morning'
        WHEN HOUR BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN HOUR BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Night'
    END AS DAY_PERIOD

FROM (
    SELECT SEQ4() AS HOUR
    FROM TABLE(GENERATOR(ROWCOUNT => 24))
)