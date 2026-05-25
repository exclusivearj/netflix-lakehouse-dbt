-- Stage users: cast types, validate tier, seed SCD2 updated_at.
{{ config(materialized='view') }}

SELECT
    CAST(user_id AS VARCHAR)        AS user_id,
    CASE
        WHEN LOWER(membership_tier) IN ('basic', 'standard', 'premium')
            THEN LOWER(membership_tier)
        ELSE 'standard'
    END                              AS membership_tier,
    region,
    device_preference,
    CAST(created_at AS DATE)         AS created_at,
    CAST(created_at AS DATE)         AS updated_at
FROM {{ ref('raw_users') }}
