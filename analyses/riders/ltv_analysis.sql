select

r.rider_id,

min(t.pickup_at) as first_trip_date,

max(t.pickup_at) as last_trip_date,

count(t.trip_id) as lifetime_trips,

sum(t.net_revenue) as lifetime_value,

avg(t.actual_fare) as avg_trip_value,

date_diff(max(t.pickup_at), min(t.pickup_at), day) as rider_lifespan_days

from {{ ref('trips_fact') }} t
left join {{ ref('riders_dim') }} r
on t.rider_id = r.rider_id

group by
r.rider_id

order by
lifetime_value desc