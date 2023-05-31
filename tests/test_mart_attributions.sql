-- test_mart_attributions.sql

-- Test that the values in the `channels` column are valid
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE channel NOT IN ('Paid Click', 'Organic Click', 'Paid Impression', 'Direct', 'Others')

UNION ALL

-- Test that the values in the `registration_dttm` column are valid
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE registration_dttm <= session_dttm

UNION ALL

-- Test that the life span of Paid Click sessions is within 3 hours
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE channel = 'Paid Click'
  AND TIMESTAMPDIFF(HOUR, session_dttm, registration_dttm) > 3

UNION ALL

-- Test that the life span of Paid Impression sessions is within 1 hour
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE channel = 'Paid Impression'
  AND TIMESTAMPDIFF(HOUR, session_dttm, registration_dttm) > 1

UNION ALL

-- Test that the life span of Organic Click sessions is within 12 hours

SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE channel = 'Organic Click'
  AND TIMESTAMPDIFF(HOUR, session_dttm, registration_dttm) > 12

UNION ALL

-- Test that Paid sessions are not hijacked by other sessions during their life span
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }} AS a
WHERE channel IN ('Paid Click', 'Paid Impression')
  AND EXISTS (
    SELECT 1
    FROM {{ ref('mart_attributions') }} AS b
    WHERE b.user_id = a.user_id
      AND b.channel != a.channel
      AND b.session_dttm <= a.session_dttm + INTERVAL '3' HOUR
      AND b.session_dttm >= a.session_dttm
  )

UNION ALL

-- Test that attributions without live sessions are either Direct or Others
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }} AS a
WHERE NOT EXISTS (
  SELECT 1
  FROM {{ ref('mart_attributions') }} AS b
  WHERE b.user_id = a.user_id
    AND (
      (b.channel IN ('Paid Click', 'Paid Impression')
        AND b.session_dttm <= a.session_dttm + INTERVAL '3' HOUR)
      OR (b.channel = 'Organic Click'
        AND b.session_dttm <= a.session_dttm + INTERVAL '12' HOUR)
    )
)
  AND a.medium NOT IN ('Direct', 'Others')

UNION ALL

-- Test that all channel are accounted for
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE channel = 'Paid Click'

UNION ALL
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE channel = 'Paid Impression'
UNION ALL
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE channel = 'Organic Click'










