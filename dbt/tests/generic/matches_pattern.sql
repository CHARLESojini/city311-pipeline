{#- Fails for non-NULL values that do not match a SQL LIKE pattern. -#}
{% test matches_pattern(model, column_name, pattern) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not like '{{ pattern }}'
{% endtest %}
