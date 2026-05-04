select

trip_id,

driver_id,

rider_id,

city_id,

surge_multiplier,

actual_fare,

extreme_surge_multiplier,

failed_payment_on_completed_trip,

case

when extreme_surge_multiplier = 1
then 'Extreme Surge Pricing'

when failed_payment_on_completed_trip = 1
then 'Failed Payment on Completed Trip'

else 'Normal'

end as fraud_flag

from {{ ref('trips_fact') }}

where

extreme_surge_multiplier = 1
or failed_payment_on_completed_trip = 1