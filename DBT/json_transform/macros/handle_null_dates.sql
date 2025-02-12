{% macro handle_null_date(column_name, default_date='9999-12-31') %}
    COALESCE({{ column_name }}, '{{ default_date }}')
{% endmacro %}
