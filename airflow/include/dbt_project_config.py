"""Cosmos config wiring the dbt project into Airflow.

Imported by lakehouse_daily_pipeline.py.
"""

from __future__ import annotations

from pathlib import Path

from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping


# The repo root is bind-mounted into the Airflow container at /usr/local/airflow/dbt.
DBT_PROJECT_DIR = Path("/usr/local/airflow/dbt")
DBT_PROFILES_DIR = DBT_PROJECT_DIR  # profiles.yml lives at project root


PROJECT_CONFIG = ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)

PROFILE_CONFIG = ProfileConfig(
    profile_name="netflix_lakehouse",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id="duckdb_default",
        profile_args={"database": "/usr/local/airflow/data/netflix_lakehouse.duckdb"},
    ),
)

BRONZE_RENDER = RenderConfig(select=["path:models/bronze"])
SILVER_RENDER = RenderConfig(select=["path:models/silver"])
GOLD_RENDER = RenderConfig(
    select=["path:models/gold"],
    exclude=["tag:skip_airflow"],
)
SNAPSHOT_RENDER = RenderConfig(select=["path:snapshots"])

EXECUTION_CONFIG = ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt")
