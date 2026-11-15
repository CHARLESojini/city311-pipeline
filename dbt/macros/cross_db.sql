{#-
  Small cross-database helpers so the same models run on Snowflake (dev/prod)
  and on DuckDB (CI, against fixture data). Each macro dispatches on the
  adapter; add an implementation here before using a new warehouse.
-#}

{# Text value of a top-level key in a semi-structured column. #}
{% macro json_str(column, key) -%}
  {{ return(adapter.dispatch('json_str', 'city311')(column, key)) }}
{%- endmacro %}

{% macro default__json_str(column, key) -%}
  nullif(trim({{ column }}:"{{ key }}"::varchar), '')
{%- endmacro %}

{% macro duckdb__json_str(column, key) -%}
  nullif(trim(json_extract_string({{ column }}, '$.{{ key }}')), '')
{%- endmacro %}


{# Parse text to a timestamp without time zone; NULL when unparseable. #}
{% macro safe_timestamp(expr) -%}
  {{ return(adapter.dispatch('safe_timestamp', 'city311')(expr)) }}
{%- endmacro %}

{% macro default__safe_timestamp(expr) -%}
  try_to_timestamp_ntz({{ expr }})
{%- endmacro %}

{% macro duckdb__safe_timestamp(expr) -%}
  try_cast({{ expr }} as timestamp)
{%- endmacro %}


{# Parse text to a double; NULL when unparseable. #}
{% macro safe_double(expr) -%}
  {{ return(adapter.dispatch('safe_double', 'city311')(expr)) }}
{%- endmacro %}

{% macro default__safe_double(expr) -%}
  try_to_double({{ expr }})
{%- endmacro %}

{% macro duckdb__safe_double(expr) -%}
  try_cast({{ expr }} as double)
{%- endmacro %}


{# Elapsed hours between two timestamps, minute precision. #}
{% macro hours_between(start_ts, end_ts) -%}
  ({{ dbt.datediff(start_ts, end_ts, 'minute') }} / 60.0)
{%- endmacro %}


{# Median / percentile of a numeric column inside a GROUP BY. #}
{% macro percentile(column, p) -%}
  {{ return(adapter.dispatch('percentile', 'city311')(column, p)) }}
{%- endmacro %}

{% macro default__percentile(column, p) -%}
  percentile_cont({{ p }}) within group (order by {{ column }})
{%- endmacro %}

{% macro duckdb__percentile(column, p) -%}
  quantile_cont({{ column }}, {{ p }})
{%- endmacro %}


{#-
  CTE definitions (no leading WITH) producing `spine(spine_date)`: one row per
  calendar day between two date expressions, inclusive. Splice into a model's
  WITH list. Built from a cross join of digits, so it needs no package and runs
  on any warehouse; covers up to 10,000 days (~27 years).
-#}
{% macro day_spine_ctes(start_date_expr, end_date_expr) -%}
  spine_digits as (
    select d from (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) as t(d)
  ),
  spine_numbers as (
    select a.d + 10 * b.d + 100 * c.d + 1000 * e.d as n
    from spine_digits a
    cross join spine_digits b
    cross join spine_digits c
    cross join spine_digits e
  ),
  spine_bounds as (
    select cast({{ start_date_expr }} as date) as lo, cast({{ end_date_expr }} as date) as hi
  ),
  spine as (
    select cast({{ dbt.dateadd('day', 'spine_numbers.n', 'spine_bounds.lo') }} as date) as spine_date
    from spine_numbers
    cross join spine_bounds
    where spine_numbers.n <= {{ dbt.datediff('spine_bounds.lo', 'spine_bounds.hi', 'day') }}
  )
{%- endmacro %}
