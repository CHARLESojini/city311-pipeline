-- Bronze: Boston legacy CRM (case_enquiry_id is a 12-digit number).
with src as (
    select * from {{ source('raw', 'boston_legacy_service_requests') }}
)

select
    {{ json_str('raw_data', 'case_enquiry_id') }}                       as request_id,
    {{ safe_timestamp(json_str('raw_data', 'open_dt')) }}               as created_at,
    {{ safe_timestamp(json_str('raw_data', 'closed_dt')) }}             as closed_at,
    {{ safe_timestamp(json_str('raw_data', 'sla_target_dt')) }}         as due_at,
    {{ json_str('raw_data', 'case_status') }}                           as status,
    {{ json_str('raw_data', 'on_time') }}                               as on_time,
    {{ json_str('raw_data', 'closure_reason') }}                        as closure_reason,
    {{ json_str('raw_data', 'case_title') }}                            as case_title,
    {{ json_str('raw_data', 'subject') }}                               as subject,
    {{ json_str('raw_data', 'reason') }}                                as reason,
    {{ json_str('raw_data', 'type') }}                                  as case_type,
    {{ json_str('raw_data', 'queue') }}                                 as queue,
    {{ json_str('raw_data', 'department') }}                            as department,
    {{ json_str('raw_data', 'source') }}                                as channel_raw,
    {{ json_str('raw_data', 'neighborhood') }}                          as neighborhood,
    {{ json_str('raw_data', 'city_council_district') }}                 as council_district,
    {{ json_str('raw_data', 'police_district') }}                       as police_district,
    {{ json_str('raw_data', 'location_zipcode') }}                      as zip_code,
    -- the portal HTML-escapes ampersands in addresses ("Armstrong St &amp; Ashley St")
    replace({{ json_str('raw_data', 'location') }}, '&amp;', '&')       as address,
    {{ safe_double(json_str('raw_data', 'latitude')) }}                 as latitude,
    {{ safe_double(json_str('raw_data', 'longitude')) }}                as longitude,
    source_file                                                         as _source_file,
    loaded_at                                                           as _loaded_at
from src
