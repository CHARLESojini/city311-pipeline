-- Gold: what is still open, and for how long, as of the latest data day.
with fct as (
    select * from {{ ref('fct_service_requests') }}
),

as_of as (
    select max(created_date) as as_of_date from fct
),

open_requests as (
    select
        fct.city,
        fct.standard_category,
        {{ dbt.datediff('fct.created_date', 'as_of.as_of_date', 'day') }} as age_days
    from fct
    cross join as_of
    where fct.status_group = 'open'
)

select
    (select as_of_date from as_of)                           as as_of_date,
    city,
    standard_category,
    count(*)                                                 as open_requests,
    sum(case when age_days > 7 then 1 else 0 end)            as open_over_7_days,
    sum(case when age_days > 30 then 1 else 0 end)           as open_over_30_days,
    {{ percentile('age_days', 0.5) }}                        as median_age_days,
    max(age_days)                                            as max_age_days
from open_requests
group by city, standard_category
