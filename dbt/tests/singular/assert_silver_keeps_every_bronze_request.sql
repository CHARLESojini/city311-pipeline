-- Dedup must collapse versions, never drop requests: every request_id seen in
-- bronze (with a usable created_at) must survive into silver exactly once.
with bronze as (
    select 'nyc' as source_system, count(distinct request_id) as n
    from {{ ref('brz_nyc_requests') }} where request_id is not null and created_at is not null
    union all
    select 'boston_legacy', count(distinct request_id)
    from {{ ref('brz_boston_legacy_requests') }} where request_id is not null and created_at is not null
    union all
    select 'boston_new', count(distinct request_id)
    from {{ ref('brz_boston_new_requests') }} where request_id is not null and created_at is not null
),

silver as (
    select source_system, count(*) as n
    from {{ ref('slv_service_requests') }}
    group by source_system
)

select b.source_system, b.n as bronze_requests, coalesce(s.n, 0) as silver_requests
from bronze b
left join silver s on s.source_system = b.source_system
where b.n <> coalesce(s.n, 0)
