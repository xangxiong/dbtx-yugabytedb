{% macro yugabytedb__refresh_materialized_view(relation) %}
    refresh materialized view {{ relation }}
{% endmacro %}
