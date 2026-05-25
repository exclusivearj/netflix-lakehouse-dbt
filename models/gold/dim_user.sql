-- SCD Type 2 user dimension — thin view over the dbt snapshot.
{{ config(materialized='table', unique_key='user_sk') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'dbt_scd_id']) }} AS user_sk,
    user_id,
    membership_tier,
    region,
    device_preference,
    created_at,
    dbt_valid_from                 AS valid_from,
    COALESCE(dbt_valid_to, TIMESTAMP '9999-12-31 00:00:00') AS valid_to,
    (dbt_valid_to IS NULL)         AS is_current
FROM {{ ref('dim_user_snapshot') }}
