-- SCD Type 2 user dimension — thin view over the dbt snapshot.
{{ config(materialized='table', unique_key='user_sk') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'dbt_scd_id']) }} AS user_sk,
    user_id,
    membership_tier,
    region,
    device_preference,
    created_at,
    -- SCD2: open each user's EARLIEST version at the beginning of time. dbt snapshots
    -- stamp dbt_valid_from = first-capture run time (e.g. 2026), but viewership events
    -- are historical (MovieLens ratings 1995-2019). Without this, fact_viewership's
    -- point-in-time join (rated_at >= valid_from) drops every historical rating. Later
    -- versions (after a membership change) keep their real dbt_valid_from.
    CASE
        WHEN dbt_valid_from = MIN(dbt_valid_from) OVER (PARTITION BY user_id)
            THEN TIMESTAMP '1900-01-01 00:00:00'
        ELSE dbt_valid_from
    END                            AS valid_from,
    COALESCE(dbt_valid_to, TIMESTAMP '9999-12-31 00:00:00') AS valid_to,
    (dbt_valid_to IS NULL)         AS is_current
FROM {{ ref('dim_user_snapshot') }}
