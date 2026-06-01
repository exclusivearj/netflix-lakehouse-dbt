"""DagBag integrity tests."""

from __future__ import annotations

import sys

import pytest


@pytest.fixture(scope="module")
def dag_bag():
    sys.path.insert(0, "/usr/local/airflow/include")
    sys.path.insert(0, "/usr/local/airflow/plugins")
    from airflow.models import DagBag

    bag = DagBag(dag_folder="/usr/local/airflow/dags", include_examples=False)
    assert bag is not None
    return bag


def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors, dag_bag.import_errors


def test_all_dags_loaded(dag_bag):
    expected = {"lakehouse_daily_pipeline", "movielens_data_refresh", "dbt_docs_publish"}
    assert expected.issubset(set(dag_bag.dag_ids))


def test_all_dags_have_tags(dag_bag):
    for dag_id in dag_bag.dag_ids:
        assert dag_bag.get_dag(dag_id).tags, dag_id


def test_lakehouse_schedule(dag_bag):
    dag = dag_bag.get_dag("lakehouse_daily_pipeline")
    assert dag.schedule_interval == "0 2 * * *" or str(dag.timetable.summary) == "0 2 * * *"


def test_data_refresh_schedule(dag_bag):
    dag = dag_bag.get_dag("movielens_data_refresh")
    assert dag.schedule_interval == "0 1 1 * *" or str(dag.timetable.summary) == "0 1 1 * *"


def test_cosmos_task_groups_present(dag_bag):
    dag = dag_bag.get_dag("lakehouse_daily_pipeline")
    task_groups = {tg.group_id for tg in dag.task_group.children.values() if hasattr(tg, "group_id")}
    assert "bronze_task_group" in task_groups or any(
        "bronze_task_group" in t.task_id for t in dag.tasks
    )


def test_observe_task_present(dag_bag):
    dag = dag_bag.get_dag("lakehouse_daily_pipeline")
    assert any("observe" in t.task_id for t in dag.tasks)
