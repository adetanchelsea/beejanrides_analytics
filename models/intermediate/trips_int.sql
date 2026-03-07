select
trip_id,

timestamp_diff(dropoff_at,pickup_at,minute) as trip_duration_minutes,

case
when is_corporate = true then 'corporate'
else 'personal'
end as corporate_trip_flag

from {{ ref('trips_stg') }}