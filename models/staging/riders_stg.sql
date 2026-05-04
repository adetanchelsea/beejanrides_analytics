select

rider_id,
date(signup_date) signup_date,
country,
referral_code,
timestamp(created_at) created_at

from {{ source('raw','riders_raw') }}

where rider_id is not null