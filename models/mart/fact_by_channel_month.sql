with
    conversions as (select * from {{ ref("stg_conversions") }}),
    attributions as (select * from {{ ref("mart_attributions") }})
select
    attribution_channel,
    date_trunc('month', registration_time) as attribution_month,
    count(distinct a.user_id) as attributed_users,
    count(*) as attributed_conversions
from attributions a
left join conversions b on a.user_id = b.user_id
group by attribution_channel, attribution_month
order by attribution_month, attribution_channel
