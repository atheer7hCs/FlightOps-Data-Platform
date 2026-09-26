with source as (
    select * from {{ source('raw_layer', 'flights_silver') }}
),

renamed as (
    select
        "icao24"                           as aircraft_id,
        "callsign"                         as flight_callsign,

        "estdepartureairport"              as departure_airport,
        "estarrivalairport"                as arrival_airport,

        to_timestamp_ntz("firstseen")      as departure_time,
        to_timestamp_ntz("lastseen")       as arrival_time,

        "estdepartureairporthorizdistance" as departure_horiz_distance_m,
        "estarrivalairporthorizdistance"   as arrival_horiz_distance_m,

        "processed_at"                      as processed_at,
         current_timestamp()                as _loaded_at
    from source
)

select * from renamed