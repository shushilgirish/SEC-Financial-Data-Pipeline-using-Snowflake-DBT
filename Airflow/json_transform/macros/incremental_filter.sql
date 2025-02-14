{% macro incremental_filter(column) %}
    {% if is_incremental() %}
        WHERE {{ column }} >= (SELECT MAX({{ column }}) FROM {{ this }})
    {% endif %}
{% endmacro %}
