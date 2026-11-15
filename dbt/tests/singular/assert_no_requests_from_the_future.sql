-- Source timestamps are local wall-clock time; allow one day of slack for
-- time-zone handling, anything later is a parsing or source bug.
select request_key, created_at
from {{ ref('fct_service_requests') }}
where created_at > {{ dbt.dateadd('day', 1, dbt.current_timestamp()) }}
