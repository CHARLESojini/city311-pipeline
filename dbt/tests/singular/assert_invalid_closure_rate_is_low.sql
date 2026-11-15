-- Closures stamped before creation exist in the NYC feed, but should stay rare.
-- Warn above 2% per source system so a systemic timestamp bug gets noticed.
{{ config(severity='warn') }}

select source_system,
       sum(case when has_invalid_closure then 1 else 0 end) * 1.0 / count(*) as invalid_rate
from {{ ref('slv_service_requests') }}
group by source_system
having sum(case when has_invalid_closure then 1 else 0 end) * 1.0 / count(*) > 0.02
