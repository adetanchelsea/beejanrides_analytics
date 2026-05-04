select

city_id,
city_name,
country,
launch_date

from {{ ref('cities_stg') }}