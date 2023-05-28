with
    sessions as (select * from {{ ref("stg_sessions") }}),
    acquisitions as (select * from {{ ref("acquisitions") }}),
select
    channel,
    date_trunc('month', registration_time) as attribution_month,
    count(distinct user_id) as attributed_users,
    count(*) as attributed_conversions
from acquisitions a
left join session_data b on a.user_id = b.user_id
group by channel, attribution_month
order by attribution_month, channel
