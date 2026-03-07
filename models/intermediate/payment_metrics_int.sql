select

trip_id,

sum(amount) as gross_revenue,
sum(fee) as processing_fee,

{{ calc_net_revenue('amount','fee') }} as net_revenue

from {{ ref('stg_payments') }}

group by trip_id