# netflix-lakehouse-dbt

> Medallion-architecture dbt project on DuckDB modeling the Netflix viewership domain. Star schema, SCD Type 2 user dimension, dbt data contracts, Bayesian-weighted content score, and Airflow + Cosmos orchestration.

This is **Project 2** in the Netflix L5 portfolio. Modeling rationale lives in [DESIGN_DOC.md](DESIGN_DOC.md) — read it alongside the code.

## Stack

| Layer | Tech |
|---|---|
| Engine | DuckDB (single-node OLAP) |
| Transform | dbt-core + dbt-duckdb |
| Source | MovieLens 25M (25M ratings, 62K movies) + synthetic users.csv |
| Orchestration | Astronomer Airflow 2.9 + astronomer-cosmos (DbtTaskGroup) |
| Data quality | [`pipeline-sentinel`](vendor/) — vendored wheel |

## Architecture

```
seeds/ (ratings, movies, tags, users)
        │
        ▼
Bronze: raw_ratings, raw_movies, raw_tags, raw_users        (views, +_loaded_at)
        │
        ▼
Silver: stg_ratings, stg_movies, stg_tags, stg_users         (cleaned + typed)
        │
        ▼
Snapshot: dim_user_snapshot (SCD Type 2)
        │
        ▼
Gold:
  dim_content (SCD1)        ─┐
  dim_user (SCD2 view)       │
  dim_date                   ├─▶  fact_viewership (grain: user × movie × event)
                             │       │ point-in-time joined to dim_user
                             │       ▼
                             └─▶  mart_content_performance (Bayesian score)
```

## One-command setup (host)

```bash
make all
```
Equivalent to:
1. `make setup` — download MovieLens 25M + generate users.csv
2. `make deps`  — `pip install -r requirements.txt && dbt deps`
3. `make build` — `dbt seed && dbt snapshot && dbt run`
4. `make test`  — `dbt test`

Then `make docs` to open the docs site.

## Airflow + Cosmos

```bash
docker compose up -d
```

Brings up postgres + airflow-init + webserver + scheduler + triggerer. UI: <http://localhost:8080> (admin/admin).

| DAG | Schedule | Purpose |
|---|---|---|
| `lakehouse_daily_pipeline` | `0 2 * * *` | seed → snapshot → Bronze (Cosmos DbtTaskGroup) → Silver → Gold → `pipeline-sentinel` quality checks on `mart_content_performance` |
| `movielens_data_refresh` | `0 1 1 * *` | monthly source refresh; triggers `lakehouse_daily_pipeline` on success |
| `dbt_docs_publish` | manual / on-success | `dbt docs generate` → copy to `/www/dbt_docs` → Slack notification |

The Cosmos `DbtTaskGroup` pattern creates **one Airflow task per dbt model** — granular retry without re-running the whole layer. Visible in Graph view.

## Key modeling decisions (DESIGN_DOC excerpts)

- **Star, not snowflake** — DuckDB is columnar; wide dim_content beats multi-join snowflake at the scales the model targets
- **SCD Type 2 only on dim_user** — membership_tier history matters for revenue attribution; movie metadata corrections are SCD1 (overwrite)
- **Point-in-time join in fact_viewership** — `rated_at BETWEEN dim_user.valid_from AND valid_to` exposes the historically-correct tier
- **Bayesian-weighted score** in `mart_content_performance` — `(C·m + n·avg) / (C + n)` prevents tiny-sample bias dominating the leaderboard

## Repository layout

```
netflix-lakehouse-dbt/
├── dbt_project.yml, profiles.yml, packages.yml
├── setup.py                       ← MovieLens download + synthetic users
├── seeds/                         ← ratings.csv, movies.csv, tags.csv, users.csv
├── models/
│   ├── bronze/   (4 views)
│   ├── silver/   (4 views — cast, regex, dedup)
│   ├── gold/     (5 tables — dim_*, fact, mart)
│   └── schema.yml                 ← data contracts (not_null, unique, relationships, accepted_values)
├── snapshots/dim_user_snapshot.sql
├── tests/                         ← 3 custom singular tests
├── analyses/sample_queries.sql    ← 8 illustrative queries
├── DESIGN_DOC.md
├── docker-compose.yml             ← Airflow + Postgres + DuckDB volume
├── Makefile                       ← setup, build, test, docs, airflow-up, ...
├── airflow/                       ← Astronomer scaffold + 3 DAGs + Cosmos config + DuckDBHook
└── vendor/pipeline_sentinel-0.1.0-py3-none-any.whl
```

## Sentinel vendoring

`pipeline-sentinel` is committed under `vendor/` and `airflow/` so the repo is fully standalone. To develop the library and this project together, replace the wheel install with editable mode:

```bash
.venv/bin/pip install -e ~/Documents/Developer/pipeline-sentinel
```

## Spec sources

`~/Documents/Developer/data-engineering-projects/files/projects/project2-lakehouse-modeling/airflow/{TASKS,TASKS_AIRFLOW,README,README_AIRFLOW}.md`
