{% macro calc_net_revenue(amount_col, fee_col) %}

sum({{ amount_col }}) - sum({{ fee_col }})

{% endmacro %}