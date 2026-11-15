-- When a city adds or renames case types (Boston's new CRM still does), they
-- fall into 'Other'. Warn when more than 40% of a system's last-28-day requests
-- are unmapped, so seeds/category_map.csv gets updated.
{{ config(severity='warn') }}

with recent as (
    select f.*
    from {{ ref('fct_service_requests') }} f
    cross join (select max(created_date) as as_of from {{ ref('fct_service_requests') }}) a
    where f.created_date > {{ dbt.dateadd('day', -28, 'a.as_of') }}
)

select source_system,
       sum(case when standard_category = 'Other' then 1 else 0 end) * 1.0 / count(*) as unmapped_share
from recent
group by source_system
having sum(case when standard_category = 'Other' then 1 else 0 end) * 1.0 / count(*) > 0.40
