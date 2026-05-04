select *

from {{ ref('trips_fact') }}

where net_revenue < 0