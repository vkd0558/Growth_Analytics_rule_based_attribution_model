-- test_mart_attributions.sql

-- Test that the values in the `channels` column are valid
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE channel NOT IN ('Paid Click', 'Organic Click', 'Paid Impression', 'Direct', 'Others')

UNION ALL

-- Test that the values in the `registration_dttm` column are valid
SELECT COUNT(*)
FROM {{ ref('mart_attributions') }}
WHERE registration_dttm < session_dttm



