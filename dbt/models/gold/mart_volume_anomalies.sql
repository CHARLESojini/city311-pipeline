-- Gold: daily request volume per series with a trailing baseline and z-score.
-- Detection is deterministic SQL; the AI layer only *explains* flagged rows.
--
-- Series are tracked at three levels so a migration reads correctly:
--   city           Boston total
--   source_system  boston_legacy falling + boston_new rising = system migration
--   category       e.g. NYC "Noise" spiking on a holiday weekend
-- Days with no requests count as zero (a feed going silent is the anomaly
-- that matters most), but only from the first day each series appears.
-- The newest day(s) are provisional: both portals publish with a lag, so the
-- latest day is usually incomplete and would read as a citywide "drop". Those
-- rows are kept (is_provisional) but never flagged; the lookback re-extract
-- completes them on the next run.
{% set baseline_window = var('anomaly_baseline_days') %}
{% set w -%}
    over (
        partition by series_type, city, series_value
        order by activity_date
        rows between {{ baseline_window }} preceding and 1 preceding
    )
{%- endset %}

with fct as (
    select * from {{ ref('fct_service_requests') }}
),

daily_series as (
    select created_date as activity_date, 'city' as series_type, city, city as series_value, count(*) as n
    from fct group by 1, 2, 3, 4
    union all
    select created_date, 'source_system', city, source_system, count(*)
    from fct group by 1, 2, 3, 4
    union all
    select created_date, 'category', city, standard_category, count(*)
    from fct group by 1, 2, 3, 4
),

series as (
    select series_type, city, series_value, min(activity_date) as first_date
    from daily_series
    group by 1, 2, 3
),

{{ day_spine_ctes('(select min(created_date) from fct)', '(select max(created_date) from fct)') }},

filled as (
    select
        s.series_type,
        s.city,
        s.series_value,
        sp.spine_date                  as activity_date,
        coalesce(d.n, 0)               as request_count
    from series s
    join spine sp
        on sp.spine_date >= s.first_date
    left join daily_series d
        on d.series_type = s.series_type
       and d.city = s.city
       and d.series_value = s.series_value
       and d.activity_date = sp.spine_date
),

baselined as (
    select
        *,
        avg(request_count) {{ w }}          as baseline_mean,
        stddev_samp(request_count) {{ w }}  as baseline_stddev,
        count(request_count) {{ w }}        as baseline_days
    from filled
),

scored as (
    select
        *,
        (request_count - baseline_mean) / nullif(baseline_stddev, 0)      as z_score,
        (request_count - baseline_mean) / nullif(baseline_mean, 0)        as pct_change_vs_baseline
    from baselined
),

latest as (
    select max(created_date) as max_date from fct
)

select
    series_type || ':' || city || ':' || series_value || ':' || cast(activity_date as varchar)
                                                                          as anomaly_key,
    activity_date,
    series_type,
    city,
    series_value,
    request_count,
    baseline_mean,
    baseline_stddev,
    baseline_days,
    z_score,
    pct_change_vs_baseline,
    case when request_count >= baseline_mean then 'spike' else 'drop' end as direction,
    activity_date > {{ dbt.dateadd('day', -var('provisional_days'), 'latest.max_date') }}
                                                                          as is_provisional,
    coalesce(
        activity_date <= {{ dbt.dateadd('day', -var('provisional_days'), 'latest.max_date') }}
        and baseline_days >= {{ (baseline_window / 2) | int }}
        and baseline_mean >= {{ var('anomaly_min_baseline') }}
        and abs(z_score) >= {{ var('anomaly_z_threshold') }}
        and abs(pct_change_vs_baseline) >= {{ var('anomaly_min_pct_change') }},
        false
    )                                                                     as is_anomaly
from scored
cross join latest
