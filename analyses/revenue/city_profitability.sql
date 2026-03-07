select

c.city_name,

count(t.trip_id) as total_trips,

sum(t.actual_fare) as gross_revenue,

sum(t.net_revenue) as net_revenue,

sum(t.actual_fare) - sum(t.net_revenue) as processing_cost,

avg(t.actual_fare) as avg_fare,

count(distinct t.rider_id) as unique_riders,

count(distinct t.driver_id) as active_drivers

from {{ ref('trips_fact') }} t
left join {{ ref('cities_dim') }} c
on t.city_id = c.city_id

group by
c.city_name

order by
net_revenue desc