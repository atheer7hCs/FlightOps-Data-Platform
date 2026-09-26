with departures as (
    select distinct departure_airport as airport_code
    from {{ ref('stg_flights') }}
    where departure_airport is not null
),

arrivals as (
    select distinct arrival_airport as airport_code
    from {{ ref('stg_flights') }}
    where arrival_airport is not null
),

all_airports as (
    select airport_code from departures
    union
    select airport_code from arrivals
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['airport_code']) }} as airport_key,
        airport_code
    from all_airports
)

select * from final