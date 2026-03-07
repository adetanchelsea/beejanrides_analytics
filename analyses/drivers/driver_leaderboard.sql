select

d.driver_id,

count(t.trip_id) as total_trips,

sum(t.net_revenue) as total_revenue,

avg(t.actual_fare) as avg_trip_value,

avg(d.rating) as driver_rating,

rank() over (
order by sum(t.net_revenue) desc
) as revenue_rank

from {{ ref('trips_fact') }} t
left join {{ ref('drivers_dim') }} d
on t.driver_id = d.driver_id

group by
d.driver_id

order by
total_revenue desc