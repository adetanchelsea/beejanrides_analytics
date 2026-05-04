select

p.payment_provider,

count(t.trip_id) as total_transactions,

sum(
    case 
        when t.failed_payment_on_completed_trip = 1
        then 1 else 0
    end
) as failed_payments,

round(
    sum(
        case 
            when t.failed_payment_on_completed_trip = 1
            then 1 else 0
        end
    )
    / count(t.trip_id) * 100
,2) as failure_rate_percentage

from {{ ref('trips_fact') }} t
left join {{ ref('payments_stg') }} p
on t.trip_id = p.trip_id

group by
p.payment_provider

order by
failure_rate_percentage desc