-- Wide, pre-aggregated mart: one row per content_sk with Bayesian-weighted score.
{{ config(materialized='table', unique_key='content_sk') }}

WITH global_stats AS (
    SELECT
        AVG(rating_value)        AS global_mean,
        AVG(rating_count_per_movie) AS avg_count
    FROM (
        SELECT
            content_sk,
            COUNT(*)           AS rating_count_per_movie,
            AVG(rating_value)  AS movie_avg,
            rating_value
        FROM {{ ref('fact_viewership') }}
        GROUP BY content_sk, rating_value
    )
),
per_movie AS (
    SELECT
        f.content_sk,
        COUNT(*)                                   AS total_ratings,
        AVG(f.rating_value)                        AS avg_rating,
        MEDIAN(f.rating_value)                     AS median_rating,
        100.0 * AVG(CASE WHEN f.is_positive THEN 1.0 ELSE 0.0 END) AS pct_positive,
        STDDEV(f.rating_value)                     AS rating_stddev,
        MIN(f.rated_at)::DATE                      AS first_rated_at,
        MAX(f.rated_at)::DATE                      AS last_rated_at,
        COUNT(DISTINCT f.user_sk)                  AS unique_raters,
        COUNT(DISTINCT u.region)                   AS unique_regions,
        SUM(CASE WHEN f.membership_tier_at_rating = 'basic'    THEN 1 ELSE 0 END) AS basic_tier_ratings,
        SUM(CASE WHEN f.membership_tier_at_rating = 'standard' THEN 1 ELSE 0 END) AS standard_tier_ratings,
        SUM(CASE WHEN f.membership_tier_at_rating = 'premium'  THEN 1 ELSE 0 END) AS premium_tier_ratings
    FROM {{ ref('fact_viewership') }} f
    LEFT JOIN {{ ref('dim_user') }} u ON u.user_sk = f.user_sk
    GROUP BY f.content_sk
)
SELECT
    pm.content_sk,
    c.movie_id,
    c.title,
    c.release_year,
    c.era,
    c.primary_genre,
    c.all_genres,
    pm.total_ratings,
    CAST(pm.avg_rating       AS DECIMAL(4,2)) AS avg_rating,
    CAST(pm.median_rating    AS DECIMAL(4,2)) AS median_rating,
    CAST(pm.pct_positive     AS DECIMAL(5,2)) AS pct_positive,
    CAST(pm.rating_stddev    AS DECIMAL(4,2)) AS rating_stddev,
    pm.first_rated_at,
    pm.last_rated_at,
    pm.unique_raters,
    pm.unique_regions,
    pm.basic_tier_ratings,
    pm.standard_tier_ratings,
    pm.premium_tier_ratings,
    -- Bayesian-weighted score: (C * m + n * avg) / (C + n)
    CAST(
        (gs.avg_count * gs.global_mean + pm.total_ratings * pm.avg_rating)
        / NULLIF(gs.avg_count + pm.total_ratings, 0)
        AS DECIMAL(6,4)
    ) AS weighted_score
FROM per_movie pm
JOIN {{ ref('dim_content') }} c ON c.content_sk = pm.content_sk
CROSS JOIN global_stats gs
