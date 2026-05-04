select

driver_id,
count(trip_id) driver_lifetime_trips

from {{ ref('trips_stg') }}

group by driver_id