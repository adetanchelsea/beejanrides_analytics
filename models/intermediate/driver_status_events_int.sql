{# 
{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}
#}

select *
from {{ ref('driver_status_events_stg') }}

{#
{% if is_incremental() %}

where event_timestamp >
(select max(event_timestamp) from {{ this }})

{% endif %}
#}