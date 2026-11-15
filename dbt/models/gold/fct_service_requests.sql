-- Gold: one row per service request (latest version), all cities.
-- The grain every mart and the dashboard drill-down build on.
select
    request_key,
    city,
    source_system,
    request_id,
    created_at,
    created_date,
    closed_at,
    closed_date,
    due_at,
    status_raw,
    status_group,
    standard_category,
    category_raw,
    subcategory_raw,
    department,
    channel,
    area,
    zip_code,
    latitude,
    longitude,
    is_on_time,
    has_invalid_closure,
    resolution_hours,
    _loaded_at
from {{ ref('slv_service_requests') }}
