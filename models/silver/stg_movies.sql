-- Stage movies: extract year from title, parse genres, derive era.
{{ config(materialized='view') }}

WITH parsed AS (
    SELECT
        CAST(movieId AS VARCHAR) AS movie_id,
        title                    AS title_raw,
        -- regex_extract returns '' when no match
        TRIM(REGEXP_REPLACE(title, '\s*\(\d{4}\)\s*$', ''))            AS title_clean,
        TRY_CAST(REGEXP_EXTRACT(title, '\((\d{4})\)\s*$', 1) AS INT)   AS release_year,
        STRING_SPLIT(genres, '|')                                       AS genres_array,
        genres
    FROM {{ ref('raw_movies') }}
)
SELECT
    movie_id,
    title_clean,
    title_raw,
    release_year,
    genres_array,
    genres_array[1]                AS primary_genre,
    LEN(genres_array)              AS genre_count,  -- DuckDB 1.5.x: CARDINALITY() is MAP-only; LEN() counts list elements
    CASE
        WHEN release_year < 1980 THEN 'classic'
        WHEN release_year < 2000 THEN 'modern'
        WHEN release_year < 2015 THEN 'contemporary'
        ELSE 'recent'
    END                            AS era
FROM parsed
WHERE release_year IS NOT NULL
  AND release_year BETWEEN 1888 AND {{ var('current_year') }}
