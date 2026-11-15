-- Bronze: Boston new CRM (case_id like BCS-00373533), live since Oct 2025.
with src as (
    select * from {{ source('raw', 'boston_new_service_requests') }}
)

select
    {{ json_str('raw_data', 'case_id') }}                               as request_id,
    {{ safe_timestamp(json_str('raw_data', 'open_date')) }}             as created_at,
    {{ safe_timestamp(json_str('raw_data', 'close_date')) }}            as closed_at,
    {{ safe_timestamp(json_str('raw_data', 'target_close_date')) }}     as due_at,
    {{ json_str('raw_data', 'case_status') }}                           as status,
    {{ json_str('raw_data', 'on_time') }}                               as on_time,
    {{ json_str('raw_data', 'closure_reason') }}                        as closure_reason,
    {{ json_str('raw_data', 'closure_comments') }}                      as closure_comments,
    {{ json_str('raw_data', 'case_topic') }}                            as case_topic,
    {{ json_str('raw_data', 'service_name') }}                          as service_name,
    {{ json_str('raw_data', 'assigned_department') }}                   as department,
    {{ json_str('raw_data', 'assigned_team') }}                         as assigned_team,
    {{ json_str('raw_data', 'report_source') }}                         as channel_raw,
    {{ json_str('raw_data', 'neighborhood') }}                          as neighborhood,
    {{ json_str('raw_data', 'city_council_district') }}                 as council_district,
    {{ json_str('raw_data', 'police_district') }}                       as police_district,
    {{ json_str('raw_data', 'zip_code') }}                              as zip_code,
    {{ json_str('raw_data', 'full_address') }}                          as address,
    {{ safe_double(json_str('raw_data', 'latitude')) }}                 as latitude,
    {{ safe_double(json_str('raw_data', 'longitude')) }}                as longitude,
    source_file                                                         as _source_file,
    loaded_at                                                           as _loaded_at
from src
