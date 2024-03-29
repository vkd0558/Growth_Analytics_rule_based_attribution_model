-- Create the sessions table
with
    session_data_s as (
        select
            user_id,
            medium,
            time_started,
            case
                when medium in ('PAID SOCIAL', 'PAID SEARCH')
                then 'Paid Click'
                when medium in ('IMPRESSION')
                then 'Paid Impression'
                when medium = 'Direct'
                then 'Direct'
                when
                    medium in (
                        'REFERRAL',
                        'ORGANIC SEARCH',
                        'DIRECT',
                        'MARKETPLACE',
                        'INVITES',
                        'PRIVATE_BOARD',
                        'OTHER',
                        'MOBILE_POPUP',
                        'SSO',
                        'SOCIAL',
                        'DIRECTORIES',
                        'MAIL'
                    )
                then 'Organic Click'
                else 'Others'
            end as attribution_channel
        from {{ ref("stg_sessions") }}
    ),

    session_data as (
        select
            user_id,
            medium,
            time_started,
            lag(time_started) over (
                partition by user_id order by time_started
            ) as previous_timestamp,
            case
                when medium = 'Paid Impression'
                then timestampadd(hour, 1, time_started)
                when medium = 'Paid Click'
                then timestampadd(hour, 3, time_started)
                when medium = 'Organic Click'
                then timestampadd(hour, 12, time_started)
            end as lifespan,
            attribution_channel
        from session_data_s
    ),
    int_sessions as (
        select
            user_id,
            medium,
            time_started,
            previous_timestamp,
            case
                when previous_timestamp is not null and lifespan is not null
                then
                    timestampadd(
                        second,
                        datediff(second, '1900-01-01', previous_timestamp),
                        lifespan
                    )
                else time_started
            end as session_end,
            attribution_channel
        from session_data
    )

select *
from int_sessions
