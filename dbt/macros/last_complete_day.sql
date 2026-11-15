{#-
  The as-of date for every gold mart: the newest day minus var('provisional_days').
  Both portals publish with a lag, so the newest day is usually incomplete and
  would understate counts. Pass the column holding the activity date.
-#}
{% macro last_complete_day(date_column='created_date') -%}
  {{ dbt.dateadd('day', -var('provisional_days'), 'max(' ~ date_column ~ ')') }}
{%- endmacro %}
