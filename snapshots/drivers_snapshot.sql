-- {% snapshot drivers_snapshot %}

-- {{
-- config(
-- target_schema='snapshots',
-- unique_key='driver_id',
-- strategy='timestamp',
-- updated_at='updated_at'
-- )
-- }}

-- select * from {{ ref('drivers_stg') }}

-- {% endsnapshot %}