"""lakehouse_daily_pipeline — daily 2am full dbt build with quality checks."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/usr/local/airflow/include")
sys.path.insert(0, "/usr/local/airflow/plugins")

from cosmos import DbtTaskGroup

from dbt_project_config import (
    BRONZE_RENDER,
    EXECUTION_CONFIG,
    GOLD_RENDER,
    PROFILE_CONFIG,
    PROJECT_CONFIG,
    SILVER_RENDER,
)
from duckdb_hook import DuckDBHook
from quality_checks import GOLD_MART_CHECKS
from slack_notifier import notify_failure

log = logging.getLogger(__name__)

DBT_DIR = "/usr/local/airflow/dbt"
DBT_BASH = (
    f"cd {DBT_DIR} && dbt {{cmd}} --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}"
)


@dag(
    dag_id="lakehouse_daily_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    # DuckDB is single-writer (one process per file). Cosmos runs each dbt model as a
    # separate `dbt` subprocess and would otherwise execute independent models (e.g. the
    # 4 bronze tables) in parallel, colliding on the DuckDB file lock. Serialize the DAG
    # so only one dbt process touches the database at a time.
    max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["project2", "dbt", "lakehouse"],
    description="Full daily run of the Netflix lakehouse: seed → snapshot → Bronze → Silver → Gold → observe quality checks.",
    on_failure_callback=notify_failure,
)
def lakehouse_daily_pipeline():
    @task
    def check_source_data_freshness() -> dict:
        hook = DuckDBHook()
        counts = {}
        # dbt-duckdb prefixes the custom 'seeds' schema with the 'main' target →
        # main_seeds. On a fresh first run these don't exist until dbt_seed runs
        # (this is a best-effort pre-flight), so missing → 0.
        for seed in ("movies", "tags", "users"):
            try:
                counts[seed] = hook.get_table_row_count("main_seeds", seed)
            except Exception as e:
                log.warning("Seed %s not yet loaded (%s); continuing.", seed, e)
                counts[seed] = 0
        # ratings is streamed from CSV via read_csv (not a seed table) — count the file.
        ratings_csv = os.environ.get("RATINGS_CSV_PATH", "seeds/ratings.csv")
        try:
            rows = hook.run(f"SELECT COUNT(*) FROM read_csv('{ratings_csv}', header=true)")
            counts["ratings"] = int(rows[0][0]) if rows else 0
        except Exception as e:
            log.warning("ratings CSV not countable (%s); continuing.", e)
            counts["ratings"] = 0
        log.info("Seed row counts: %s", counts)
        return counts

    dbt_seed_task = BashOperator(
        task_id="dbt_seed",
        bash_command=DBT_BASH.format(cmd="seed"),
    )

    bronze_task_group = DbtTaskGroup(
        group_id="bronze_task_group",
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=BRONZE_RENDER,
    )

    silver_task_group = DbtTaskGroup(
        group_id="silver_task_group",
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=SILVER_RENDER,
    )

    dim_user_snapshot_task = BashOperator(
        task_id="dim_user_snapshot",
        bash_command=DBT_BASH.format(cmd="snapshot --select dim_user_snapshot"),
    )

    gold_task_group = DbtTaskGroup(
        group_id="gold_task_group",
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=GOLD_RENDER,
    )

    @task
    def run_observe_quality_checks() -> dict:
        hook = DuckDBHook()
        # Gold models land in main_gold (dbt-duckdb prefixes the custom 'gold' schema
        # with the 'main' target), NOT a bare 'gold' schema.
        if not hook.table_exists("main_gold", "mart_content_performance"):
            raise AirflowSkipException("mart_content_performance not built; skipping checks.")
        df = hook.get_pandas_df("SELECT * FROM main_gold.mart_content_performance")

        results: list[dict] = []
        any_fail = False
        for check in GOLD_MART_CHECKS:
            r = check._safe_evaluate(df)
            results.append(
                {
                    "check": r.check_name,
                    "status": r.status.value,
                    "column": r.column,
                    "message": r.message,
                }
            )
            if r.status.value == "fail":
                any_fail = True
        log.info("Observe results: %s", results)
        return {"status": "fail" if any_fail else "pass", "results": results, "rows": len(df)}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def store_run_metadata(seed_counts: dict, observe_result: dict) -> None:
        # ALL_DONE means this runs even if an upstream task was skipped/failed, in which
        # case its XCom arrives as None — guard so .get() doesn't blow up.
        seed_counts = seed_counts or {}
        observe_result = observe_result or {}
        hook = DuckDBHook()
        conn = hook.get_conn()
        try:
            conn.execute(
                """
                CREATE SCHEMA IF NOT EXISTS pipeline_audit;
                CREATE TABLE IF NOT EXISTS pipeline_audit.dbt_runs (
                    run_id VARCHAR,
                    execution_date TIMESTAMP,
                    status VARCHAR,
                    seed_ratings_rows INT,
                    seed_movies_rows INT,
                    seed_tags_rows INT,
                    seed_users_rows INT,
                    gold_mart_rows INT,
                    observe_status VARCHAR
                );
                """
            )
            conn.execute(
                "INSERT INTO pipeline_audit.dbt_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "{{ run_id }}",
                    datetime.utcnow(),
                    observe_result.get("status", "unknown"),
                    seed_counts.get("ratings", 0),
                    seed_counts.get("movies", 0),
                    seed_counts.get("tags", 0),
                    seed_counts.get("users", 0),
                    observe_result.get("rows", 0),
                    observe_result.get("status", "unknown"),
                ],
            )
        finally:
            conn.close()

    freshness = check_source_data_freshness()
    observe_result = run_observe_quality_checks()
    metadata = store_run_metadata(freshness, observe_result)

    freshness >> dbt_seed_task >> bronze_task_group >> silver_task_group
    silver_task_group >> dim_user_snapshot_task >> gold_task_group >> observe_result
    observe_result >> metadata


dag = lakehouse_daily_pipeline()
