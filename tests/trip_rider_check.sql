select *
from {{ ref('trips_fact') }}
where rider_id is null