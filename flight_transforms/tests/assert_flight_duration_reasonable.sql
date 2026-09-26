select
    aircraft_id,
    flight_callsign,
    flight_duration_minutes
from {{ ref('fct_flights') }}
where flight_duration_minutes > 1440