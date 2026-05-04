select

driver_id,
date(onboarding_date) onboarding_date,
driver_status,
city_id,
vehicle_id,
cast(rating as numeric) rating,
timestamp(created_at) created_at,
timestamp(updated_at) updated_at

from {{ source('raw','drivers_raw') }}

where driver_id is not null