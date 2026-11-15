-- Gold: how fast each city resolves each standard category, over the trailing
-- performance window. Shared categories make NYC vs Boston comparable
-- (e.g. potholes, street lights, illegal dumping).
{% set window_days = var('performance_window_days') %}

with fct as (
    select * from {{ ref('fct_service_requests') }}
),

as_of as (
    select max(created_date) as as_of_date from fct
),

windowed as (
    select fct.*
    from fct
    cross join as_of
    where fct.created_date >  {{ dbt.dateadd('day', -window_days, 'as_of.as_of_date') }}
      and fct.created_date <= as_of.as_of_date
)

select
    (select as_of_date from as_of)                                  as as_of_date,
    {{ window_days }}                                               as window_days,
    city,
    standard_category,
    count(*)                                                        as requests,
    sum(case when status_group = 'closed' then 1 else 0 end)        as closed_requests,
    sum(case when status_group = 'closed' then 1 else 0 end) * 1.0
        / count(*)                                                  as pct_closed,
    {{ percentile('resolution_hours', 0.5) }}                       as median_resolution_hours,
    {{ percentile('resolution_hours', 0.9) }}                       as p90_resolution_hours,
    -- Boston only: NYC does not publish a service-level target
    avg(case when is_on_time then 1.0 when not is_on_time then 0.0 end)
                                                                    as pct_on_time,
    sum(case when has_invalid_closure then 1 else 0 end)            as invalid_closures
from windowed
group by city, standard_category
