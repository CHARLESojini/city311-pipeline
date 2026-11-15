-- Gold: requests opened and closed per day, city, source system and category.
-- Drives the dashboard trend chart and the Tableau export.
with fct as (
    select * from {{ ref('fct_service_requests') }}
),

opened as (
    select
        created_date as activity_date, city, source_system, standard_category,
        count(*)     as opened_count,
        0            as closed_count,
        cast(null as double) as resolution_hours_sum
    from fct
    group by 1, 2, 3, 4
),

closed as (
    select
        closed_date  as activity_date, city, source_system, standard_category,
        0            as opened_count,
        count(*)     as closed_count,
        sum(resolution_hours) as resolution_hours_sum
    from fct
    where closed_date is not null
    group by 1, 2, 3, 4
)

select
    activity_date,
    city,
    source_system,
    standard_category,
    sum(opened_count)                                         as opened_count,
    sum(closed_count)                                         as closed_count,
    sum(resolution_hours_sum) / nullif(sum(closed_count), 0)  as avg_resolution_hours_closed
from (
    select * from opened
    union all
    select * from closed
) activity
group by 1, 2, 3, 4
