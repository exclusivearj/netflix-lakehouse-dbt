-- SCD Type 1 content dimension (overwrite-on-change semantics).
{{ config(materialized='table', unique_key='content_sk') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['movie_id']) }} AS content_sk,
    movie_id,
    title_clean                          AS title,
    title_raw,
    release_year,
    era,
    primary_genre,
    ARRAY_TO_STRING(genres_array, ', ')  AS all_genres,
    genre_count,
    (genre_count > 1)                    AS is_multi_genre,
    CURRENT_TIMESTAMP                    AS dbt_updated_at
FROM {{ ref('stg_movies') }}
