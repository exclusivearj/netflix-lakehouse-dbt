-- Stage ratings: cast types, derive flags, filter invalid rows.
{{ config(materialized='view') }}

SELECT
    CAST(userId AS VARCHAR)        AS user_id,
    CAST(movieId AS VARCHAR)       AS movie_id,
    CAST(rating AS DECIMAL(3,1))   AS rating_value,
    TO_TIMESTAMP(timestamp)        AS rated_at,
    (CAST(rating AS DECIMAL(3,1)) >= 3.5) AS is_positive
FROM {{ ref('raw_ratings') }}
WHERE rating BETWEEN 0.5 AND 5.0
  AND timestamp IS NOT NULL
