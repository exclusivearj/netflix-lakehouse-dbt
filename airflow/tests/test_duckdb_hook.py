"""Unit tests for DuckDBHook — uses a real temp DuckDB file per test."""

from __future__ import annotations

import json
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, "/usr/local/airflow/plugins")
sys.path.insert(0, "airflow/plugins")


@pytest.fixture
def hook(tmp_path, monkeypatch):
    from duckdb_hook import DuckDBHook

    db = tmp_path / "test.duckdb"

    # Stub get_connection to return an object with our extra blob
    class _Conn:
        extra = json.dumps({"database": str(db)})

    monkeypatch.setattr(DuckDBHook, "get_connection", lambda self, _: _Conn())
    return DuckDBHook()


def test_hook_connects_to_duckdb(hook):
    conn = hook.get_conn()
    try:
        assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        conn.close()


def test_run_returns_rows(hook):
    assert hook.run("SELECT 42")[0][0] == 42


def test_get_pandas_df_returns_dataframe(hook):
    df = hook.get_pandas_df("SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2, 'b'")
    assert list(df.columns) == ["x", "y"]
    assert len(df) == 2


def test_table_exists_returns_false_for_missing_table(hook):
    assert hook.table_exists("public", "missing") is False


def test_table_exists_returns_true_after_creation(hook):
    conn = hook.get_conn()
    try:
        conn.execute("CREATE TABLE public.t1 (x INT)")
    finally:
        conn.close()
    assert hook.table_exists("public", "t1") is True


def test_get_table_row_count(hook):
    conn = hook.get_conn()
    try:
        conn.execute("CREATE TABLE public.t2 (x INT)")
        conn.execute("INSERT INTO public.t2 VALUES (1), (2), (3)")
    finally:
        conn.close()
    assert hook.get_table_row_count("public", "t2") == 3
