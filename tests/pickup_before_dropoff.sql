select *
from {{ ref('trips_fact') }}
where dropoff_at < pickup_at