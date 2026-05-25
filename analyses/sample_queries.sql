/*
  Eight analytical queries demonstrating the Gold star schema.
  Each block is independently runnable via dbt compile + manual execution
  (or via your DuckDB CLI):    duckdb data/netflix_lakehouse.duckdb < query.sql
*/

-- 1. Top 10 content by avg rating (min 1000 ratings)
SELECT title, primary_genre, era, avg_rating, total_ratings, weighted_score
FROM {{ ref('mart_content_performance') }}
WHERE total_ratings >= 1000
ORDER BY weighted_score DESC, total_ratings DESC
LIMIT 10;

-- 2. Rating trends by year (with YoY delta)
WITH yearly AS (
    SELECT rating_year, AVG(rating_value) AS avg_rating, COUNT(*) AS n
    FROM {{ ref('fact_viewership') }}
    GROUP BY rating_year
)
SELECT
    rating_year,
    avg_rating,
    n,
    avg_rating - LAG(avg_rating) OVER (ORDER BY rating_year) AS yoy_delta
FROM yearly
ORDER BY rating_year;

-- 3. Genre popularity (total ratings + avg rating per genre, ranked)
SELECT
    primary_genre,
    SUM(total_ratings)             AS total_ratings,
    AVG(avg_rating)                AS avg_rating_across_titles,
    COUNT(*)                       AS title_count
FROM {{ ref('mart_content_performance') }}
GROUP BY primary_genre
ORDER BY total_ratings DESC;

-- 4. Premium vs basic vs standard tier rating behavior
SELECT
    membership_tier_at_rating       AS tier,
    AVG(rating_value)               AS avg_rating,
    COUNT(*)                        AS rating_count,
    AVG(CASE WHEN is_positive THEN 1.0 ELSE 0.0 END) AS pct_positive
FROM {{ ref('fact_viewership') }}
GROUP BY tier
ORDER BY rating_count DESC;

-- 5. Era analysis (avg + count by era)
SELECT
    era,
    AVG(avg_rating)        AS era_avg_rating,
    SUM(total_ratings)     AS era_ratings,
    COUNT(*)               AS title_count
FROM {{ ref('mart_content_performance') }}
GROUP BY era
ORDER BY era_ratings DESC;

-- 6. User cohort analysis: casual / moderate / power viewers
WITH per_user AS (
    SELECT
        user_sk,
        COUNT(*)            AS n_ratings,
        AVG(rating_value)   AS avg_rating
    FROM {{ ref('fact_viewership') }}
    GROUP BY user_sk
)
SELECT
    CASE
        WHEN n_ratings <  10 THEN 'casual'
        WHEN n_ratings <= 50 THEN 'moderate'
        ELSE                       'power'
    END                                 AS cohort,
    COUNT(*)                            AS users,
    AVG(avg_rating)                     AS avg_user_rating,
    AVG(n_ratings)                      AS avg_n_ratings
FROM per_user
GROUP BY cohort
ORDER BY cohort;

-- 7. Controversial content (high stddev + min 500 ratings)
SELECT title, primary_genre, total_ratings, avg_rating, rating_stddev
FROM {{ ref('mart_content_performance') }}
WHERE rating_stddev > 1.5
  AND total_ratings >= 500
ORDER BY rating_stddev DESC
LIMIT 20;

-- 8. SCD2 validation: users who changed membership tier (before/after)
SELECT
    user_id,
    LIST_AGG(membership_tier ORDER BY valid_from) AS tier_history,
    COUNT(*)                                       AS rows_in_dim_user
FROM {{ ref('dim_user') }}
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY rows_in_dim_user DESC
LIMIT 20;
