-- Test passes when zero rows returned: every fact_viewership.content_sk
-- must resolve to a dim_content row.
SELECT f.rating_sk, f.content_sk
FROM {{ ref('fact_viewership') }} f
LEFT JOIN {{ ref('dim_content') }} c USING (content_sk)
WHERE c.content_sk IS NULL
