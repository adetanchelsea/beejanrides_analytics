with source as (

    select * 
    from {{ source('raw','trips_raw') }}

),

cleaned as (

    select

        trip_id,
        rider_id,
        driver_id,
        vehicle_id,
        city_id,

        timestamp(requested_at) as requested_at,
        timestamp(pickup_at) as pickup_at,
        timestamp(dropoff_at) as dropoff_at,

        status,

        SAFE_CAST(estimated_fare AS NUMERIC) AS estimated_fare,
        SAFE_CAST(actual_fare AS NUMERIC) AS actual_fare,

        SAFE_CAST(surge_multiplier AS NUMERIC) AS surge_multiplier,

        payment_method,
        is_corporate,

        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at

    from source
    where trip_id is not null

)

select *
from cleaned