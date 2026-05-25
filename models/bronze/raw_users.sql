-- Raw passthrough of users.csv seed; adds a load timestamp.
{{ config(materialized='view') }}

SELECT
    user_id,
    membership_tier,
    region,
    device_preference,
    created_at,
    CURRENT_TIMESTAMP AS _loaded_at
FROM {{ ref('users') }}
