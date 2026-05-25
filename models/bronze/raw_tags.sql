-- Raw passthrough of tags.csv seed; adds a load timestamp.
{{ config(materialized='view') }}

SELECT
    userId,
    movieId,
    tag,
    timestamp,
    CURRENT_TIMESTAMP AS _loaded_at
FROM {{ ref('tags') }}
