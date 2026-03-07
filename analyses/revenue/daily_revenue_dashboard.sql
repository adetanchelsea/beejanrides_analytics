select
    date(t.pickup_at) as trip_date,
    c.city_name,
    count(t.trip_id) as total_trips,
    sum(t.actual_fare) as gross_revenue,
    sum(t.net_revenue) as net_revenue,
    avg(t.actual_fare) as avg_trip_fare

from {{ ref('trips_fact') }} as t
left join {{ ref('cities_dim') }} as c
    on t.city_id = c.city_id

group by
    date(t.pickup_at),
    c.city_name

order by
    trip_date,
    c.city_name