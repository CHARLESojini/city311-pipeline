-- Bronze: NYC raw JSON flattened to typed columns. One row per loaded record
-- version; duplicates (re-extracts, overlapping windows) are kept here and
-- resolved in silver.
with src as (
    select * from {{ source('raw', 'nyc_service_requests') }}
)

select
    {{ json_str('raw_data', 'unique_key') }}                                         as request_id,
    {{ safe_timestamp(json_str('raw_data', 'created_date')) }}                       as created_at,
    {{ safe_timestamp(json_str('raw_data', 'closed_date')) }}                        as closed_at,
    {{ safe_timestamp(json_str('raw_data', 'resolution_action_updated_date')) }}     as resolution_updated_at,
    {{ json_str('raw_data', 'status') }}                                             as status,
    {{ json_str('raw_data', 'agency') }}                                             as agency,
    {{ json_str('raw_data', 'agency_name') }}                                        as agency_name,
    {{ json_str('raw_data', 'complaint_type') }}                                     as complaint_type,
    {{ json_str('raw_data', 'descriptor') }}                                         as descriptor,
    {{ json_str('raw_data', 'location_type') }}                                      as location_type,
    {{ json_str('raw_data', 'resolution_description') }}                             as resolution_description,
    {{ json_str('raw_data', 'open_data_channel_type') }}                             as channel_raw,
    {{ json_str('raw_data', 'borough') }}                                            as borough,
    {{ json_str('raw_data', 'community_board') }}                                    as community_board,
    {{ json_str('raw_data', 'council_district') }}                                   as council_district,
    {{ json_str('raw_data', 'incident_zip') }}                                       as zip_code,
    {{ json_str('raw_data', 'incident_address') }}                                   as address,
    {{ safe_double(json_str('raw_data', 'latitude')) }}                              as latitude,
    {{ safe_double(json_str('raw_data', 'longitude')) }}                             as longitude,
    source_file                                                                      as _source_file,
    loaded_at                                                                        as _loaded_at
from src
