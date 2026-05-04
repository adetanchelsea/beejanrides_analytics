select

driver_id,
onboarding_date,
driver_status,
city_id,
vehicle_id,
rating

from {{ ref('drivers_stg') }}