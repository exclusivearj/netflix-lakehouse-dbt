{% snapshot dim_user_snapshot %}
{{
    config(
      target_schema='gold',
      unique_key='user_id',
      strategy='check',
      check_cols=['membership_tier', 'region', 'device_preference'],
    )
}}
SELECT
    user_id,
    membership_tier,
    region,
    device_preference,
    created_at,
    updated_at
FROM {{ ref('stg_users') }}
{% endsnapshot %}
