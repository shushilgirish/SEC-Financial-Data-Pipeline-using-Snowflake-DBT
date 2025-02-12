{% test not_null_except_zero(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
AND fy != 0
{% endtest %}
