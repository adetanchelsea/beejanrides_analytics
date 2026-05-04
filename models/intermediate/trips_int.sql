select

trip_id,

{{ calc_trip_duration('pickup_at','dropoff_at') }} as trip_duration_minutes,

case
when is_corporate = true then 'corporate'
else 'personal'
end as corporate_trip_flag

from {{ ref('trips_stg') }}