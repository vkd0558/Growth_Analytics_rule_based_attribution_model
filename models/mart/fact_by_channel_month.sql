with
    sessions as (select * from {{ ref("int_sessions") }}),
    acquisitions as (select * from {{ ref("mart_attributions") }}),
select
    channel,
    date_trunc('month', registration_time) as attribution_month,
    count(distinct user_id) as attributed_users,
    count(*) as attributed_conversions
from acquisitions a
left join session_data b on a.user_id = b.user_id
group by channel, attribution_month
order by attribution_month, channel
