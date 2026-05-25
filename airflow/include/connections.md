# Airflow Connections

| Conn ID | Type | Used For |
|---|---|---|
| `duckdb_default` | Generic | `extra` = `{"database": "/usr/local/airflow/data/netflix_lakehouse.duckdb"}` |
| `slack_webhook` | HTTP | Slack alerts on test/quality failures (host: `hooks.slack.com`, password: webhook URL) |
| `movielens_http` | HTTP | MovieLens download (host: `files.grouplens.org`, schema: `https`) |

In `docker-compose.yml` these are wired via `AIRFLOW_CONN_*` env vars for local dev; configure real values in Airflow UI for staging/prod.
