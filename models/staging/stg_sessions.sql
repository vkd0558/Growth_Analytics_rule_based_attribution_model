

-- Create the sessions table
WITH session_data AS (
  SELECT
    user_id,
    medium,
    timestamp,
    LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) AS previous_timestamp,
    CASE
      WHEN medium = 'Paid Impression' THEN INTERVAL '1 hour'
      WHEN medium = 'Paid Click' THEN INTERVAL '3 hours'
      WHEN medium = 'Organic Click' THEN INTERVAL '12 hours'
      ELSE NULL
    END AS lifespan
  from {{ source("sessions", "sessions") }}}
),

stg_sessions AS (
  SELECT
    user_id,
    medium,
    timestamp,
    previous_timestamp,
    COALESCE(previous_timestamp + lifespan, timestamp) AS session_end
  FROM session_data
)

SELECT * FROM stg_sessions;

