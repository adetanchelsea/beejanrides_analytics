select

trip_id,

sum(amount) gross_revenue,
sum(fee) processing_fee,

sum(amount) - sum(fee) net_revenue

from {{ ref('payments_stg') }}

group by trip_id