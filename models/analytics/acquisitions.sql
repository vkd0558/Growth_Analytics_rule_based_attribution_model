with sessions as {
    select * from {{ ref('stg_sessions')}}
},
with conversions as {
    select * from {{ ref('stg_conversions')}}
}
-- Create the conversions table
WITH attribution_data AS (
  SELECT
    c.user_id,
    c.timestamp AS conversion_timestamp,
    s.medium AS attribution_medium,
    s.session_end,
    CASE
      WHEN s.medium IN ('Paid Impression', 'Paid Click') THEN TRUE
      ELSE FALSE
    END AS is_paid_session
  FROM conversions c
  LEFT JOIN sessions s ON c.user_id = s.user_id
  WHERE c.timestamp <= s.session_end
),

attributions AS (
  SELECT
    user_id,
    CASE
      WHEN is_paid_session = FALSE THEN 'Organic Click'
      WHEN attribution_medium = 'Direct' THEN 'Direct'
      WHEN attribution_medium IS NULL THEN 'Others'
      ELSE attribution_medium
    END AS attribution_channel
  FROM attribution_data
)

SELECT * FROM attributions;
