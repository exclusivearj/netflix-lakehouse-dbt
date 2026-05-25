"""movielens_data_refresh — monthly download + validation + downstream trigger."""

from __future__ import annotations

import hashlib
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/usr/local/airflow/include")

from slack_notifier import notify_failure

log = logging.getLogger(__name__)

MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-25m.zip"
DATA_DIR = Path("/usr/local/airflow/data")
ZIP_PATH = DATA_DIR / "ml-25m.zip"


@dag(
    dag_id="movielens_data_refresh",
    start_date=datetime(2024, 1, 1),
    schedule="0 1 1 * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["project2", "ingestion"],
    description="Monthly MovieLens 25M refresh; auto-triggers lakehouse pipeline.",
    on_failure_callback=notify_failure,
)
def movielens_data_refresh():
    @task
    def check_for_update() -> bool:
        r = requests.head(MOVIELENS_URL, timeout=15, allow_redirects=True)
        r.raise_for_status()
        etag = r.headers.get("ETag", "")
        last_seen = Variable.get("movielens_last_etag", default_var="")
        if etag and etag == last_seen:
            log.info("No update — ETag unchanged: %s", etag)
            return False
        log.info("Update available (etag old=%s new=%s)", last_seen, etag)
        return True

    @task
    def download_movielens(has_update: bool) -> str:
        if not has_update:
            log.info("Skipping download — no update.")
            return str(ZIP_PATH)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with requests.get(MOVIELENS_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length") or 0)
            downloaded = 0
            next_log = 10
            with ZIP_PATH.open("wb") as fh:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fh.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = 100 * downloaded / total
                        if pct >= next_log:
                            log.info("  download progress: %.0f%%", pct)
                            next_log += 10
            new_etag = r.headers.get("ETag", "")
            if new_etag:
                Variable.set("movielens_last_etag", new_etag)
        log.info("Downloaded %d bytes to %s", downloaded, ZIP_PATH)
        return str(ZIP_PATH)

    @task
    def validate_checksums(zip_path: str) -> str:
        p = Path(zip_path)
        if not p.exists() or p.stat().st_size == 0:
            raise AirflowException(f"{zip_path} missing or empty")
        sha = hashlib.sha256()
        with p.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                sha.update(chunk)
        digest = sha.hexdigest()
        Variable.set("movielens_sha256", digest)
        log.info("SHA-256: %s", digest)
        return digest

    @task
    def extract_and_copy(zip_path: str) -> int:
        import shutil
        import zipfile

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(DATA_DIR)
        extracted = DATA_DIR / "ml-25m"
        seeds = Path("/usr/local/airflow/dbt/seeds")
        seeds.mkdir(parents=True, exist_ok=True)
        copied = 0
        for name in ("ratings.csv", "movies.csv", "tags.csv"):
            src = extracted / name
            if src.exists():
                shutil.copyfile(src, seeds / name)
                copied += 1
        return copied

    @task
    def generate_synthetic_users() -> int:
        sys.path.insert(0, "/usr/local/airflow/dbt")
        try:
            import setup as project_setup  # type: ignore
        except Exception as e:
            log.warning("setup module not importable in this image (%s); skipping users.csv refresh", e)
            return 0
        return project_setup.generate_users()

    trigger_lakehouse = TriggerDagRunOperator(
        task_id="trigger_lakehouse_dag",
        trigger_dag_id="lakehouse_daily_pipeline",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    has_update = check_for_update()
    zip_path = download_movielens(has_update)
    sha = validate_checksums(zip_path)
    copied = extract_and_copy(zip_path)
    users = generate_synthetic_users()
    sha >> copied >> users >> trigger_lakehouse


dag = movielens_data_refresh()
