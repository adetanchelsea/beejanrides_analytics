select

event_id,
driver_id,
status,
timestamp(event_timestamp) event_timestamp

from {{ source('raw','driver_status_events_raw') }}
