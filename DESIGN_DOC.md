# DESIGN DOC — Lakehouse Modeling for Streaming Content Domain

**Author:** Akshay Jain
**Date:** (fill in)
**Status:** Draft

---

## 1. Problem Statement

Model the viewership and content domain for a Netflix-like streaming platform so that:
- Analysts can answer business questions about content performance without writing complex SQL
- ML engineers can derive user engagement features from the Gold layer directly
- Historical accuracy is preserved — queries can reconstruct state as of any past date
- The model is self-documenting via dbt and data contracts

---

## 2. Source Data Profile

| Source Table | Rows | Key Columns | Quality Notes |
|---|---|---|---|
| ratings.csv | 25M | userId, movieId, rating, timestamp | No nulls; rating ∈ [0.5, 5.0] in 0.5 steps |
| movies.csv | 62K | movieId, title, genres | genres is pipe-delimited; year embedded in title |
| tags.csv | 1M | userId, movieId, tag, timestamp | Free-text; duplicates exist |

---

## 3. Modeling Decisions

### 3.1 Star Schema vs. Snowflake

**Decision: Star Schema (denormalized dims)**

Rationale:
- Query performance: DuckDB is a columnar OLAP engine — wide tables scan faster than multi-join snowflakes
- Analyst ergonomics: fewer joins for the most common queries
- Netflix's own data culture values fast, self-service analytics — star schema serves that
- Trade-off accepted: some redundancy in `dim_content` (genre repeated per movie)

Snowflake would be preferred if: content metadata changed frequently (high SCD churn), or storage cost at extreme scale was a concern. At 62K movies, denormalization wins.

### 3.2 SCD Strategy by Dimension

| Dimension | SCD Type | Rationale |
|---|---|---|
| dim_content | Type 1 (overwrite) | Movie metadata rarely changes; release year / genres are immutable. Corrections should overwrite. |
| dim_user | Type 2 (history) | Membership tier changes are analytically significant — revenue attribution requires knowing what tier a user was on when they rated a title. |
| dim_date | Type 0 (static) | Calendar attributes never change by definition. |

### 3.3 Why NOT SCD Type 2 for dim_content?

If a movie's genre classification is corrected (e.g., reclassified from Action to Action|Thriller), we want all historical fact rows to reflect the corrected classification — not preserve the "wrong" old state. This is a data quality correction, not a business event. Therefore Type 1 (overwrite) is correct.

Exception: If we were tracking content licensing windows (available_from / available_to per region), that would warrant Type 2. Out of scope here but noted for a follow-up.

### 3.4 Grain of fact_viewership

Grain: **one row per (user, content, rating event)**

This is finer than "one row per user+content" because a user can re-rate a movie. The `rating_sk` surrogate key ensures uniqueness. Analysts who want the "latest rating" should filter using a window function or use `mart_content_performance` which pre-aggregates.

### 3.5 Partitioning Strategy

`fact_viewership` is partitioned by `rating_year` (derived from `rated_at`).

Rationale:
- Most analytical queries filter by time period
- DuckDB partition pruning eliminates full scans on large date ranges
- Monthly partitioning would be too granular for 25M rows; yearly is appropriate

### 3.6 Surrogate Keys

All dimension surrogate keys use `dbt_utils.generate_surrogate_key()` (MD5 hash of natural key fields). This ensures:
- Deterministic key values (idempotent `dbt run`)
- No dependency on database sequences
- Portable across DuckDB, BigQuery, Snowflake

---

## 4. Data Contracts

Every model in the Gold layer has a `schema.yml` contract enforcing:

```
dim_content:
  - content_sk: not_null, unique
  - movie_id: not_null, unique
  - release_year: not_null, between(1888, current_year)
  - primary_genre: not_null, accepted_values([...])

fact_viewership:
  - rating_sk: not_null, unique
  - user_sk: not_null, relationships(dim_user.user_sk)
  - content_sk: not_null, relationships(dim_content.content_sk)
  - date_sk: not_null, relationships(dim_date.date_sk)
  - rating_value: not_null, between(0.5, 5.0)
```

Referential integrity between facts and dims is enforced via `relationships` tests.
The `assert_no_orphan_facts.sql` singular test is a belt-and-suspenders check.

---

## 5. ML Feature Readiness

The Gold layer is designed so ML feature pipelines can pull directly from it:

| ML Use Case | Feature Source | Notes |
|---|---|---|
| Content-based filtering | dim_content.all_genres, release_year, era | No joins needed |
| Collaborative filtering | fact_viewership (userId, movieId, rating) | Grain is correct for matrix factorization |
| User engagement scoring | mart_content_performance | Pre-aggregated avg_rating, rating_count per content |
| Recency weighting | fact_viewership.days_since_release | Pre-computed at fact grain |

---

## 6. What Would Change at Netflix Scale

| This Project | At Netflix Scale |
|---|---|
| DuckDB (single node) | Apache Spark on EMR / Dataproc |
| dbt-duckdb | dbt-spark or dbt-bigquery |
| CSV seeds | Kafka → Bronze layer via Flink/Spark Streaming |
| Daily dbt run | Hourly incremental models with `is_incremental()` |
| SCD2 via dbt snapshot | Custom merge logic in Spark for sub-second SCD |
| Single partition column | Multi-column partitioning (year, month, content_type) |

---

## 7. Open Questions / Future Work

1. **Tags integration**: `stg_tags` is modeled but not yet joined into the Gold layer. A `dim_tag` + `bridge_content_tag` would enable tag-based content discovery — good follow-up task.
2. **User demographics**: MovieLens 25M does not include demographic data. In a real Netflix model, `dim_user` would include age_group, country, device_preference — all candidates for SCD2.
3. **Content availability windows**: Licensing data (when a title becomes/stops being available) would require a `fact_content_availability` table with date-effective rows — a natural extension.
