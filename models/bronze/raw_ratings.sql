-- Raw passthrough of ratings.csv; adds a load timestamp.
-- Streamed via read_csv (NOT a dbt seed): the 25M-row seed COPY buffers the whole
-- load in one transaction and OOM-killed the ~6GB Airflow container, whereas a
-- streaming read_csv stays memory-bounded (spills to disk). The CSV still lives in
-- seeds/ but is disabled as a seed (see dbt_project.yml). movies/tags/users remain
-- seeds. Path is overridable via RATINGS_CSV_PATH so the Airflow container can pass an
-- absolute path; on the host it defaults to the project-relative seeds/ratings.csv.
{{ config(materialized='view') }}

SELECT
    userId,
    movieId,
    rating,
    timestamp,
    CURRENT_TIMESTAMP AS _loaded_at
FROM read_csv(
    '{{ env_var("RATINGS_CSV_PATH", "seeds/ratings.csv") }}',
    header = true,
    columns = {
        'userId': 'BIGINT',
        'movieId': 'BIGINT',
        'rating': 'DOUBLE',
        'timestamp': 'BIGINT'
    }
)
