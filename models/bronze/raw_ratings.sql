-- Raw passthrough of ratings.csv seed; adds a load timestamp.
{{ config(materialized='view') }}

SELECT
    userId,
    movieId,
    rating,
    timestamp,
    CURRENT_TIMESTAMP AS _loaded_at
FROM {{ ref('ratings') }}
