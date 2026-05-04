select

t.trip_id,
t.driver_id,
t.rider_id,
t.city_id,

t.requested_at,
t.pickup_at,
t.dropoff_at,

m.trip_duration_minutes,

t.actual_fare,
t.surge_multiplier,
t.is_corporate,

p.net_revenue,

f.failed_payment_on_completed_trip,
f.extreme_surge_multiplier

from {{ ref('trips_stg') }} t
left join {{ ref('trips_int') }} m using(trip_id)
left join {{ ref('payment_metrics_int') }} p using(trip_id)
left join {{ ref('fraud_indicators_int') }} f using(trip_id)
