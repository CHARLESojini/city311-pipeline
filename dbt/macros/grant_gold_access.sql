{#-
  on-run-end hook: after a prod run on Snowflake, let downstream roles read GOLD.
  dbt (role TRANSFORMER) owns the GOLD schema, so it is the one that can grant.
  Roles come from var gold_reader_roles; no-op on dev, CI and DuckDB.
-#}
{% macro grant_gold_access() %}
  {% if execute and target.name == 'prod' and target.type == 'snowflake' %}
    {% set schema = target.database ~ '.GOLD' %}
    {% for role in var('gold_reader_roles', []) %}
      grant usage on schema {{ schema }} to role {{ role }};
      grant select on all tables in schema {{ schema }} to role {{ role }};
      grant select on all views in schema {{ schema }} to role {{ role }};
    {% endfor %}
  {% endif %}
{% endmacro %}
