with
    sessions as (select * from {{ ref('int_sessions') }}),
    conversions as (select * from {{ ref('stg_conversions') }}),
    attribution_data AS (
    SELECT
      c.user_id,
      c.registration_time AS conversion_timestamp,
      s.medium AS attribution_medium,
      s.time_started,
      s.attribution_channel,
      CASE
        WHEN s.attribution_channel IN ('Paid Impression', 'Paid Click') THEN TRUE
        ELSE FALSE
      END AS is_paid_session
    FROM conversions c
    LEFT JOIN sessions s ON c.user_id = s.user_id
    WHERE c.registration_time >= s.time_started
  ),
  paid_click_sessions AS (
    SELECT
      user_id,
      time_started,
      DATEADD('hour', 3, time_started) AS paid_click_expiration_time
    FROM attribution_data
    WHERE attribution_channel = 'Paid Click'
  ),
  paid_impression_sessions AS (
    SELECT
      user_id,
      time_started,
      DATEADD('hour', 1, time_started) AS paid_impression_expiration_time
    FROM attribution_data
    WHERE attribution_channel = 'Paid Impression'
  ),
  organic_click_sessions AS (
    SELECT
      user_id,
      time_started,
      DATEADD('hour', 12, time_started) AS organic_click_expiration_time
    FROM attribution_data
    WHERE attribution_channel = 'Organic Click'
  ),
  valid_attributions AS (
    SELECT
      attribution_data.user_id,
      attribution_data.conversion_timestamp,
      attribution_data.time_started,
      attribution_data.attribution_medium,
      attribution_data.attribution_channel,
      CASE
        -- Scenario 1: Paid Click within 3 hours
        WHEN attribution_data.attribution_channel = 'Paid Click'
          AND attribution_data.conversion_timestamp <= pc.paid_click_expiration_time
          AND attribution_data.time_started <= pc.paid_click_expiration_time
        THEN 'Valid'
        -- Scenario 2: Paid Impression within 1 hour
        WHEN attribution_data.attribution_channel = 'Paid Impression'
          AND attribution_data.conversion_timestamp <= pi.paid_impression_expiration_time
          AND attribution_data.time_started <= pi.paid_impression_expiration_time
        THEN 'Valid'
        -- Scenario 3: Organic Click within 12 hours, not hijacked by paid sessions
        WHEN attribution_data.attribution_channel = 'Organic Click'
          AND attribution_data.conversion_timestamp <= oc.organic_click_expiration_time
          AND attribution_data.time_started <= oc.organic_click_expiration_time
          AND (
            NOT EXISTS (
              SELECT 1
              FROM paid_click_sessions pc
              WHERE pc.user_id = attribution_data.user_id
                AND attribution_data.time_started <= pc.paid_click_expiration_time
            )
            OR (
              EXISTS (
                SELECT 1
                FROM paid_click_sessions pc
                WHERE pc.user_id = attribution_data.user_id
                  AND attribution_data.time_started <= pc.paid_click_expiration_time
              )
              AND EXISTS (
                SELECT 1
                FROM paid_impression_sessions pi
                WHERE pi.user_id = attribution_data.user_id
                  AND pi.time_started > attribution_data.time_started
                  AND pi.time_started <= pc.paid_click_expiration_time
              )
            )
          )
        THEN 'Valid'
        -- Scenario 4: No live sessions (paid or organic), assign credit to Direct or Others
        WHEN attribution_data.attribution_channel IS NULL
          AND (
            attribution_data.attribution_medium = 'Direct'
            OR (
              attribution_data.attribution_medium IS NULL
              AND NOT EXISTS (
                SELECT 1
                FROM sessions s
                WHERE s.user_id = attribution_data.user_id
              )
            )
          )
        THEN 'Valid'
        ELSE 'Invalid'
      END AS attribution_validation,
      CASE
        -- Assign credit to Paid Click if valid and last touch
        WHEN attribution_data.attribution_channel = 'Paid Click' AND attribution_validation = 'Valid' THEN 1.0
        -- Assign credit to Paid Impression if valid and last touch
        WHEN attribution_data.attribution_channel = 'Paid Impression' AND attribution_validation = 'Valid' THEN 1.0
        -- Assign credit to Organic Click if valid and not hijacked
        WHEN attribution_data.attribution_channel = 'Organic Click' AND attribution_validation = 'Valid' THEN 1.0
        ELSE 0.0
      END AS credit
    FROM attribution_data
    LEFT JOIN paid_click_sessions pc ON attribution_data.user_id = pc.user_id
    LEFT JOIN paid_impression_sessions pi ON attribution_data.user_id = pi.user_id
    LEFT JOIN organic_click_sessions oc ON attribution_data.user_id = oc.user_id
  )
SELECT USER_ID,
TIME_STARTED as SESSION_DTTM,
CONVERSION_TIMESTAMP as REGISTRATION_DTTM,
ATTRIBUTION_MEDIUM AS MEDIUM,
ATTRIBUTION_CHANNEL AS CHANNEL,
CASE 
WHEN credit =0 then 'FALSE'
WHEN credit =1 then 'TRUE'
ELSE NULL
END AS IS_PAID
FROM valid_attributions
