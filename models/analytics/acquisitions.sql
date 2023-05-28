with sessions as (
    select * from {{ ref('stg_sessions') }}
),
conversions as (
    select * from {{ ref('stg_conversions') }}
),
attribution_data as (
    select
        c.user_id,
        c.registration_time as conversion_timestamp,
        s.medium as attribution_medium,
        s.session_end,
        case
            when s.medium in ('Paid Impression', 'Paid Click') then true
            else false
        end as is_paid_session
    from conversions c
    left join sessions s on c.user_id = s.user_id
    where c.registration_time <= s.session_end
),
attributions as (
    select
        user_id,
        case
            when is_paid_session = false then 'Organic Click'
            when attribution_medium = 'Direct' then 'Direct'
            when attribution_medium is null then 'Others'
            else attribution_medium
        end as attribution_channel
    from attribution_data
)
select *
from attributions;
