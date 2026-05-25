-- Raw passthrough of movies.csv seed; adds a load timestamp.
{{ config(materialized='view') }}

SELECT
    movieId,
    title,
    genres,
    CURRENT_TIMESTAMP AS _loaded_at
FROM {{ ref('movies') }}
