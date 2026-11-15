-- Silver: latest version of every NYC request, in the conformed column set
-- shared by all sources (see slv_service_requests).
with latest as (
    select *
    from {{ ref('brz_nyc_requests') }}
    where request_id is not null
      and created_at is not null
    qualify row_number() over (
        partition by request_id
        order by _loaded_at desc, _source_file desc, closed_at desc nulls last
    ) = 1
)

select
    'NYC'                   as city,
    'nyc'                   as source_system,
    request_id,
    created_at,
    closed_at,
    cast(null as timestamp) as due_at,
    status                  as status_raw,
    complaint_type          as category_raw,
    descriptor              as subcategory_raw,
    agency                  as department,
    channel_raw,
    borough                 as area,
    zip_code,
    address,
    latitude,
    longitude,
    cast(null as varchar)   as on_time_raw,
    _source_file,
    _loaded_at
from latest
