-- Calendar dimension covering 1995-01-01 to 2025-12-31.
-- Must span the full rated_at range (MovieLens ratings start 1995) or
-- fact_viewership.date_sk fails its relationship test to dim_date.
{{ config(materialized='table') }}

WITH spine AS (
    SELECT CAST(generate_series AS DATE) AS full_date
    FROM generate_series(DATE '1995-01-01', DATE '2025-12-31', INTERVAL '1 day')
)
SELECT
    CAST(STRFTIME(full_date, '%Y%m%d') AS INT) AS date_sk,
    full_date,
    CAST(STRFTIME(full_date, '%Y') AS INT)     AS year,
    -- DuckDB '/' is float division (1/3 = 0.33); use '//' for integer quarters 1-4
    ((CAST(STRFTIME(full_date, '%m') AS INT) - 1) // 3 + 1) AS quarter,
    'Q' || (((CAST(STRFTIME(full_date, '%m') AS INT) - 1) // 3 + 1))::VARCHAR AS quarter_name,
    CAST(STRFTIME(full_date, '%m') AS INT)     AS month,
    STRFTIME(full_date, '%B')                  AS month_name,
    STRFTIME(full_date, '%b')                  AS month_short,
    CAST(STRFTIME(full_date, '%W') AS INT)     AS week_of_year,
    CAST(STRFTIME(full_date, '%d') AS INT)     AS day_of_month,
    -- ISO day-of-week: Monday=1, Sunday=7
    CASE WHEN ((DAYOFWEEK(full_date) + 6) % 7) = 0
         THEN 7 ELSE ((DAYOFWEEK(full_date) + 6) % 7) END AS day_of_week,
    STRFTIME(full_date, '%A')                  AS day_name,
    DAYOFWEEK(full_date) IN (0, 6)             AS is_weekend,
    ((CAST(STRFTIME(full_date, '%Y') AS INT) % 4 = 0
      AND CAST(STRFTIME(full_date, '%Y') AS INT) % 100 <> 0)
     OR CAST(STRFTIME(full_date, '%Y') AS INT) % 400 = 0)   AS is_leap_year,
    DAY(LAST_DAY(full_date))                    AS days_in_month
FROM spine
