-- Fact: one row per (user, movie, rating event); point-in-time joined to dim_user.
{{ config(materialized='table') }}

WITH r AS (
    SELECT * FROM {{ ref('stg_ratings') }}
),
c AS (
    SELECT content_sk, movie_id, release_year FROM {{ ref('dim_content') }}
),
u AS (
    SELECT user_sk, user_id, membership_tier, valid_from, valid_to
    FROM {{ ref('dim_user') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['r.user_id', 'r.movie_id', 'r.rated_at']) }} AS rating_sk,
    u.user_sk,
    c.content_sk,
    CAST(STRFTIME(r.rated_at, '%Y%m%d') AS INT)                       AS date_sk,
    r.rating_value,
    r.is_positive,
    r.rated_at,
    CAST(STRFTIME(r.rated_at, '%Y') AS INT)                           AS rating_year,
    CAST(STRFTIME(r.rated_at, '%m') AS INT)                           AS rating_month,
    CASE
        WHEN c.release_year IS NULL THEN NULL
        ELSE DATEDIFF('day', MAKE_DATE(c.release_year, 1, 1), r.rated_at)
    END                                                                AS days_since_release,
    u.membership_tier                                                  AS membership_tier_at_rating
FROM r
JOIN c ON c.movie_id = r.movie_id
JOIN u ON u.user_id = r.user_id
       AND r.rated_at >= u.valid_from
       AND r.rated_at < u.valid_to
