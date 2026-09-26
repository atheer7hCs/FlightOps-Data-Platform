
{{
    config(
        materialized='incremental',
        unique_key=['aircraft_id', 'departure_time']
    )
}}

with flights as (
    select * from {{ ref('stg_flights') }}
),

departure_airports as (
    select * from {{ ref('dim_airports') }}
),

arrival_airports as (
    select * from {{ ref('dim_airports') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

joined as (
    select
        f.aircraft_id,
        f.flight_callsign,
        f.departure_time,
        f.arrival_time,

        dep.airport_key as departure_airport_key,
        arr.airport_key as arrival_airport_key,
        d.date_key as departure_date_key,

        datediff(
            'minute',
            f.departure_time,
            f.arrival_time
        ) as flight_duration_minutes,

        f.processed_at as processed_at,
        f._loaded_at as _loaded_at

    from flights f

    left join departure_airports dep
        on f.departure_airport = dep.airport_code

    left join arrival_airports arr
        on f.arrival_airport = arr.airport_code

    left join dates d
        on cast(f.departure_time as date) = d.date_day
),

final as (
    select
        *,
        
        row_number() over (
            partition by aircraft_id, departure_time
            order by arrival_time desc
        ) as row_num,

        rank() over (
            partition by departure_airport_key
            order by flight_duration_minutes desc
        ) as duration_rank_from_airport

    from joined

    where flight_duration_minutes > 0
)


select final.*
from final
where final.row_num = 1

{% if is_incremental() %}

and final._loaded_at > (
    select max(t._loaded_at)
    from {{ this }} as t
)

{% endif %}