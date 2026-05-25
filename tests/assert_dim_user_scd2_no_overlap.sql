-- Test passes when zero rows returned: SCD2 windows for the same user_id
-- must never overlap.
SELECT
    a.user_id,
    a.user_sk AS a_sk,
    b.user_sk AS b_sk,
    a.valid_from AS a_from, a.valid_to AS a_to,
    b.valid_from AS b_from, b.valid_to AS b_to
FROM {{ ref('dim_user') }} a
JOIN {{ ref('dim_user') }} b
  ON a.user_id = b.user_id
 AND a.user_sk <> b.user_sk
WHERE a.valid_from < b.valid_to
  AND a.valid_to   > b.valid_from
