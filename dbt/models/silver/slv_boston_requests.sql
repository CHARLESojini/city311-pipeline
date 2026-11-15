-- Silver: Boston as one continuous history across the CRM migration.
-- Legacy (12-digit IDs) and new (BCS- IDs) cases never share an ID, so the two
-- systems are deduped independently and stacked; source_system keeps them apart.
with legacy as (
    select *
    from {{ ref('brz_boston_legacy_requests') }}
    where request_id is not null
      and created_at is not null
    qualify row_number() over (
        partition by request_id
        order by _loaded_at desc, _source_file desc, closed_at desc nulls last
    ) = 1
),

new_crm as (
    select *
    from {{ ref('brz_boston_new_requests') }}
    where request_id is not null
      and created_at is not null
    qualify row_number() over (
        partition by request_id
        order by _loaded_at desc, _source_file desc, closed_at desc nulls last
    ) = 1
)

select
    'Boston'          as city,
    'boston_legacy'   as source_system,
    request_id,
    created_at,
    closed_at,
    due_at,
    status            as status_raw,
    case_type         as category_raw,
    reason            as subcategory_raw,
    department,
    channel_raw,
    neighborhood      as area,
    zip_code,
    address,
    latitude,
    longitude,
    on_time           as on_time_raw,
    _source_file,
    _loaded_at
from legacy

union all

select
    'Boston'          as city,
    'boston_new'      as source_system,
    request_id,
    created_at,
    closed_at,
    due_at,
    status            as status_raw,
    service_name      as category_raw,
    case_topic        as subcategory_raw,
    department,
    channel_raw,
    neighborhood      as area,
    zip_code,
    address,
    latitude,
    longitude,
    on_time           as on_time_raw,
    _source_file,
    _loaded_at
from new_crm
