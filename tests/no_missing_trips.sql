select t.trip_id
from {{ ref('trips_stg') }} t
left join {{ ref('trips_fact') }} f
on t.trip_id = f.trip_id
where f.trip_id is null