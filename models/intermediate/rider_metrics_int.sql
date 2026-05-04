select

rider_id,
sum(actual_fare) rider_lifetime_value

from {{ ref('trips_stg') }}

group by rider_id