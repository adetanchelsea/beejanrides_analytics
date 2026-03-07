{% macro calc_trip_duration(start_col, end_col) %}
timestamp_diff({{ end_col }}, {{ start_col }}, minute)
{% endmacro %}