-- Test passes when zero rows returned: rating_value must be in [0.5, 5.0].
SELECT rating_sk, rating_value
FROM {{ ref('fact_viewership') }}
WHERE rating_value NOT BETWEEN 0.5 AND 5.0
