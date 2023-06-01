
with
    attributions as (select * from {{ ref("analy_attributions") }})
SELECT
  channel,
  TO_CHAR(DATE_TRUNC('month', REGISTRATION_DTTM), 'Mon') AS attribution_month,
  COUNT(DISTINCT user_id) AS attributed_users,
  COUNT(CASE WHEN is_paid = 'TRUE' THEN 1 END) AS attributed_conversions
FROM attributions
GROUP BY channel, attribution_month
ORDER BY attribution_month, channel;
