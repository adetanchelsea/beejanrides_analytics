select

t.trip_id,

case
when p.payment_status = 'failed'
and t.status = 'completed'
then 1 else 0
end as failed_payment_on_completed_trip,

case
when t.surge_multiplier > 10
then 1 else 0
end as extreme_surge_multiplier,

case
when count(p.payment_id) over(partition by t.trip_id) > 1
then 1 else 0
end as duplicate_trip_payments

from {{ ref('trips_stg') }} t
left join {{ ref('payments_stg') }} p
on t.trip_id = p.trip_id