-- test_mart_attributions.sql

-- Test that the values in the `channels` column are valid
SELECT COUNT(*)
FROM {{ ref('analy_attributions') }}
WHERE channel NOT IN ('Paid Click', 'Organic Click', 'Paid Impression', 'Direct', 'Others')

UNION ALL

-- Test that the values in the `registration_dttm` column are valid
SELECT COUNT(*)
FROM {{ ref('analy_attributions') }}
WHERE registration_dttm <= session_dttm

UNION ALL

-- Test that the life span of Paid Click sessions is within 3 hours
SELECT COUNT(*)
FROM {{ ref('analy_attributions') }}
WHERE channel = 'Paid Click'
  AND TIMESTAMPDIFF(HOUR, session_dttm, registration_dttm) > 3

UNION ALL

-- Test that the life span of Paid Impression sessions is within 1 hour
SELECT COUNT(*)
FROM {{ ref('analy_attributions') }}
WHERE channel = 'Paid Impression'
  AND TIMESTAMPDIFF(HOUR, session_dttm, registration_dttm) > 1

UNION ALL

-- Test that the life span of Organic Click sessions is within 12 hours

SELECT COUNT(*)
FROM {{ ref('analy_attributions') }}
WHERE channel = 'Organic Click'
  AND TIMESTAMPDIFF(HOUR, session_dttm, registration_dttm) > 12

UNION ALL

-- Test that Paid sessions are not hijacked by other sessions during their life span
SELECT COUNT(*)
FROM {{ ref('analy_attributions') }} AS a
WHERE channel IN ('Paid Click', 'Paid Impression')
  AND EXISTS (
    SELECT 1
    FROM {{ ref('analy_attributions') }} AS b
    WHERE b.user_id = a.user_id
      AND b.channel != a.channel
      AND b.session_dttm <= DATEADD('hour', 3, a.session_dttm)
      AND b.session_dttm >= a.session_dttm
  ) AND IS_PAID='TRUE'

UNION ALL

SELECT COUNT(*)
FROM {{ ref('analy_attributions') }} AS a
LEFT JOIN {{ ref('analy_attributions') }} AS b
    ON a.user_id = b.user_id
    AND (
        (b.channel IN ('Paid Click', 'Paid Impression') AND b.session_dttm <= DATEADD('hour', 3, a.session_dttm))
        OR (b.channel = 'Organic Click' AND b.session_dttm <= DATEADD('hour', 12, a.session_dttm))
    )




