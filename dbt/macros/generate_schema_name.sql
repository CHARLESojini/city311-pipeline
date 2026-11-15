{#-
  prod: models land in the plain layer schemas (BRONZE, SILVER, GOLD, REFERENCE)
  that the dashboard and AI layer read.
  dev/ci: layer schemas are prefixed with the target schema (e.g. DBT_DEV_SILVER),
  so development never overwrites production tables.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- elif target.name == 'prod' -%}
    {{ custom_schema_name | trim | upper }}
  {%- else -%}
    {{ target.schema }}_{{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
