-- The two Boston CRMs are told apart by ID format: legacy cases are 12-digit
-- numbers, new-system cases start with BCS-. A mismatch means a feed got
-- routed to the wrong table or the city changed its ID scheme.
select source_system, request_id
from {{ ref('slv_boston_requests') }}
where (source_system = 'boston_legacy' and not {{ regex_full_match('request_id', '[0-9]{12}') }})
   or (source_system = 'boston_new'    and not {{ regex_full_match('request_id', 'BCS-[0-9]+') }})
