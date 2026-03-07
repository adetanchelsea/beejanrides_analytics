select

city_id,
city_name,
country,
date(launch_date) launch_date

from {{ source('raw','cities_raw') }}