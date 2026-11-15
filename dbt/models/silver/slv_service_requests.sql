-- Silver: every city and source system in one conformed schema.
-- Think of it as a travel adapter: each source keeps its own plug shape in
-- bronze, and this model gives them one common socket.
with unioned as (
    select * from {{ ref('slv_nyc_requests') }}
    union all
    select * from {{ ref('slv_boston_requests') }}
),

categories as (
    select * from {{ ref('category_map') }}
),

channels as (
    select * from {{ ref('channel_map') }}
),

enriched as (
    select
        u.source_system || ':' || u.request_id                          as request_key,
        u.city,
        u.source_system,
        u.request_id,
        u.created_at,
        u.closed_at,
        u.due_at,
        u.status_raw,
        case
            when lower(u.status_raw) = 'closed' then 'closed'
            when lower(u.status_raw) in ('error', 'unspecified') then 'other'
            else 'open'
        end                                                             as status_group,
        u.category_raw,
        u.subcategory_raw,
        coalesce(c.standard_category, 'Other')                          as standard_category,
        u.department,
        coalesce(ch.channel, 'unknown')                                 as channel,
        nullif(u.area, 'Unspecified')                                   as area,
        -- Boston publishes ZIPs as numbers (02135 -> 2135): restore the leading zero
        case
            when u.zip_code is null then null
            else lpad(split_part(u.zip_code, '.', 1), 5, '0')
        end                                                             as zip_code,
        u.address,
        -- drop coordinates outside the Northeast US (zeros, swapped signs, junk)
        case when u.latitude between 40 and 43.5 and u.longitude between -75 and -70
             then u.latitude end                                        as latitude,
        case when u.latitude between 40 and 43.5 and u.longitude between -75 and -70
             then u.longitude end                                       as longitude,
        case upper(u.on_time_raw)
            when 'ONTIME' then true
            when 'OVERDUE' then false
        end                                                             as is_on_time,
        u._source_file,
        u._loaded_at
    from unioned u
    left join categories c
        on c.source_system = u.source_system
       and c.category_raw = u.category_raw
    left join channels ch
        on ch.source_system = u.source_system
       and ch.channel_raw = u.channel_raw
)

select
    *,
    -- NYC has closures stamped before creation (and 1900-01-01 placeholders)
    closed_at is not null and closed_at < created_at                   as has_invalid_closure,
    case
        when closed_at is not null and closed_at >= created_at
            then {{ hours_between('created_at', 'closed_at') }}
    end                                                                 as resolution_hours,
    cast(created_at as date)                                            as created_date,
    case when closed_at >= created_at then cast(closed_at as date) end  as closed_date
from enriched
