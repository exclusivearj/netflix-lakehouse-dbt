-- Stage tags: normalize, dedup, filter empties.
{{ config(materialized='view') }}

WITH normalized AS (
    SELECT
        CAST(userId AS VARCHAR)             AS user_id,
        CAST(movieId AS VARCHAR)            AS movie_id,
        TRIM(LOWER(tag))                    AS tag_normalized,
        TO_TIMESTAMP(timestamp)             AS tagged_at,
        ROW_NUMBER() OVER (
            PARTITION BY userId, movieId, TRIM(LOWER(tag))
            ORDER BY timestamp DESC
        )                                    AS rn
    FROM {{ ref('raw_tags') }}
    WHERE tag IS NOT NULL AND TRIM(tag) <> ''
)
SELECT user_id, movie_id, tag_normalized, tagged_at
FROM normalized
WHERE rn = 1
