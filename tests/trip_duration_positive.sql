select *

from {{ ref('trips_fact') }}

where trip_duration_minutes <= 0