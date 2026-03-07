select

rider_id,
signup_date,
country,
referral_code

from {{ ref('riders_stg') }}