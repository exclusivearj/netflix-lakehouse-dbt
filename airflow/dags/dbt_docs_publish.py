"""dbt_docs_publish — generate + serve dbt docs (triggered)."""

from __future__ import annotations

import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

sys.path.insert(0, "/usr/local/airflow/include")

from slack_notifier import notify_failure, post

log = logging.getLogger(__name__)

DBT_DIR = "/usr/local/airflow/dbt"
TARGET_DIR = Path("/usr/local/airflow/dbt/target")
DOCS_OUT = Path("/usr/local/airflow/www/dbt_docs")


@dag(
    dag_id="dbt_docs_publish",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["project2", "documentation"],
    description="Generate dbt docs and copy to www volume; notify Slack.",
    on_failure_callback=notify_failure,
)
def dbt_docs_publish():
    generate_dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=(
            f"cd {DBT_DIR} && dbt docs generate "
            f"--profiles-dir {DBT_DIR} --project-dir {DBT_DIR}"
        ),
    )

    @task
    def copy_docs_to_volume() -> int:
        DOCS_OUT.mkdir(parents=True, exist_ok=True)
        count = 0
        for path in TARGET_DIR.iterdir():
            if path.is_file() and path.suffix in {".html", ".json"}:
                shutil.copyfile(path, DOCS_OUT / path.name)
                count += 1
        log.info("Copied %d files to %s", count, DOCS_OUT)
        return count

    @task
    def notify_docs_ready(file_count: int) -> None:
        post(f":books: dbt docs updated ({file_count} files) — http://localhost:8080/dbt_docs")

    file_count = copy_docs_to_volume()
    notify_docs_ready(file_count)
    generate_dbt_docs >> file_count


dag = dbt_docs_publish()
