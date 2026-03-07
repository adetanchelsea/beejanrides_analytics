select

payment_id,
trip_id,
payment_status,
payment_provider,
cast(amount as numeric) amount,
cast(fee as numeric) fee,
currency,
timestamp(created_at) created_at

from {{ source('raw','payments_raw') }}

where payment_id is not null