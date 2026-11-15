-- Gold: one headline row per city for the dashboard KPI tiles.
with fct as (
    select * from {{ ref('fct_service_requests') }}
),

as_of as (
    select max(created_date) as as_of_date from fct
),

flagged as (
    select
        fct.*,
        fct.created_date >  {{ dbt.dateadd('day', -7, 'as_of.as_of_date') }}       as in_last_7,
        fct.created_date <= {{ dbt.dateadd('day', -7, 'as_of.as_of_date') }}
            and fct.created_date > {{ dbt.dateadd('day', -14, 'as_of.as_of_date') }} as in_prior_7,
        fct.created_date >  {{ dbt.dateadd('day', -28, 'as_of.as_of_date') }}      as in_last_28
    from fct
    cross join as_of
)

select
    (select as_of_date from as_of)                                          as as_of_date,
    city,
    sum(case when in_last_7 then 1 else 0 end)                              as requests_last_7d,
    sum(case when in_prior_7 then 1 else 0 end)                             as requests_prior_7d,
    (sum(case when in_last_7 then 1 else 0 end)
        - sum(case when in_prior_7 then 1 else 0 end)) * 1.0
        / nullif(sum(case when in_prior_7 then 1 else 0 end), 0)            as pct_change_7d,
    {{ percentile('case when in_last_28 then resolution_hours end', 0.5) }} as median_resolution_hours_28d,
    avg(case when in_last_28 and is_on_time then 1.0
             when in_last_28 and not is_on_time then 0.0 end)              as pct_on_time_28d,
    sum(case when status_group = 'open' then 1 else 0 end)                  as open_requests,
    count(distinct source_system)                                           as source_systems
from flagged
group by city
