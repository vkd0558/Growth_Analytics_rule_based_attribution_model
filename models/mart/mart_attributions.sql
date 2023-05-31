with
    sessions as (select * from {{ ref("int_sessions") }}),
    conversions as (select * from {{ ref("stg_conversions") }}),
    attribution_data as (
        select
            c.user_id,
            c.registration_time as conversion_timestamp,
            s.medium as attribution_medium,
            s.time_started,
            s.attribution_channel,
            case
                when s.attribution_channel in ('Paid Impression', 'Paid Click')
                then true
                else false
            end as is_paid_session
        from conversions c
        left join sessions s on c.user_id = s.user_id
        where c.registration_time >= s.time_started
    ),
    paid_click_sessions as (
        select
            user_id,
            time_started,
            dateadd('hour', 3, time_started) as paid_click_expiration_time
        from attribution_data
        where attribution_channel = 'Paid Click'
    ),
    paid_impression_sessions as (
        select
            user_id,
            time_started,
            dateadd('hour', 1, time_started) as paid_impression_expiration_time
        from attribution_data
        where attribution_channel = 'Paid Impression'
    ),
    organic_click_sessions as (
        select
            user_id,
            time_started,
            dateadd('hour', 12, time_started) as organic_click_expiration_time
        from attribution_data
        where attribution_channel = 'Organic Click'
    ),
    valid_attributions as (
        select
            attribution_data.user_id,
            attribution_data.conversion_timestamp,
            attribution_data.time_started,
            attribution_data.attribution_medium,
            attribution_data.attribution_channel,
            case
                -- Scenario 1: Paid Click within 3 hours
                when
                    attribution_data.attribution_channel = 'Paid Click'
                    and attribution_data.conversion_timestamp
                    <= pc.paid_click_expiration_time
                    and attribution_data.time_started <= pc.paid_click_expiration_time
                then 'Valid'
                -- Scenario 2: Paid Impression within 1 hour
                when
                    attribution_data.attribution_channel = 'Paid Impression'
                    and attribution_data.conversion_timestamp
                    <= pi.paid_impression_expiration_time
                    and attribution_data.time_started
                    <= pi.paid_impression_expiration_time
                then 'Valid'
                -- Scenario 3: Organic Click within 12 hours, not hijacked by paid
                -- sessions
                when
                    attribution_data.attribution_channel = 'Organic Click'
                    and attribution_data.conversion_timestamp
                    <= oc.organic_click_expiration_time
                    and attribution_data.time_started
                    <= oc.organic_click_expiration_time
                    and (
                        not exists (
                            select 1
                            from paid_click_sessions pc
                            where
                                pc.user_id = attribution_data.user_id
                                and attribution_data.time_started
                                <= pc.paid_click_expiration_time
                        )
                        or (
                            exists (
                                select 1
                                from paid_click_sessions pc
                                where
                                    pc.user_id = attribution_data.user_id
                                    and attribution_data.time_started
                                    <= pc.paid_click_expiration_time
                            )
                            and exists (
                                select 1
                                from paid_impression_sessions pi
                                where
                                    pi.user_id = attribution_data.user_id
                                    and pi.time_started > attribution_data.time_started
                                    and pi.time_started <= pc.paid_click_expiration_time
                            )
                        )
                    )
                then 'Valid'
                -- Scenario 4: No live sessions (paid or organic), assign credit to
                -- Direct or Others
                when
                    attribution_data.attribution_channel is null
                    and (
                        attribution_data.attribution_medium = 'Direct'
                        or (
                            attribution_data.attribution_medium is null
                            and not exists (
                                select 1
                                from sessions s
                                where s.user_id = attribution_data.user_id
                            )
                        )
                    )
                then 'Valid'
                else 'Invalid'
            end as attribution_validation,
            case
                -- Assign credit to Paid Click if valid and last touch
                when
                    attribution_data.attribution_channel = 'Paid Click'
                    and attribution_validation = 'Valid'
                then 1.0
                -- Assign credit to Paid Impression if valid and last touch
                when
                    attribution_data.attribution_channel = 'Paid Impression'
                    and attribution_validation = 'Valid'
                then 1.0
                -- Assign credit to Organic Click if valid and not hijacked
                when
                    attribution_data.attribution_channel = 'Organic Click'
                    and attribution_validation = 'Valid'
                then 1.0
                else 0.0
            end as credit
        from attribution_data
        left join paid_click_sessions pc on attribution_data.user_id = pc.user_id
        left join paid_impression_sessions pi on attribution_data.user_id = pi.user_id
        left join organic_click_sessions oc on attribution_data.user_id = oc.user_id
    )
select
    user_id,
    time_started as session_dttm,
    conversion_timestamp as registration_dttm,
    attribution_medium as medium,
    attribution_channel as channel,
    case
        when credit = 0 then 'FALSE' when credit = 1 then 'TRUE' else null
    end as is_paid
from valid_attributions
