"""DuckDBHook — Airflow hook wrapping duckdb.connect()."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import pandas as pd
from airflow.hooks.base import BaseHook


log = logging.getLogger(__name__)


class DuckDBHook(BaseHook):
    """Airflow Hook for DuckDB.

    Reads connection `duckdb_default`. The DuckDB file path comes from
    the connection's `extra` JSON (`{"database": "..."}`). When the
    connection isn't configured it falls back to the `DUCKDB_PATH`
    environment variable, then to `:memory:`.
    """

    conn_name_attr = "duckdb_conn_id"
    default_conn_name = "duckdb_default"
    conn_type = "duckdb"
    hook_name = "DuckDB"

    def __init__(self, duckdb_conn_id: str = "duckdb_default") -> None:
        super().__init__()
        self.duckdb_conn_id = duckdb_conn_id

    def _resolve_database(self) -> str:
        try:
            conn = self.get_connection(self.duckdb_conn_id)
            extra = json.loads(conn.extra or "{}")
            db = extra.get("database")
            if db:
                return db
        except Exception:
            pass
        return os.environ.get("DUCKDB_PATH", ":memory:")

    def get_conn(self):
        import duckdb

        db = self._resolve_database()
        if db != ":memory:":
            os.makedirs(os.path.dirname(db) or ".", exist_ok=True)
        return duckdb.connect(db)

    def run(self, sql: str, parameters: Optional[list[Any]] = None) -> list[tuple]:
        conn = self.get_conn()
        try:
            cur = conn.execute(sql, parameters or [])
            return cur.fetchall()
        finally:
            conn.close()

    def get_pandas_df(self, sql: str) -> pd.DataFrame:
        conn = self.get_conn()
        try:
            return conn.execute(sql).fetchdf()
        finally:
            conn.close()

    def get_table_row_count(self, schema: str, table: str) -> int:
        rows = self.run(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        return int(rows[0][0]) if rows else 0

    def get_schema_info(self, schema: str) -> list[dict]:
        rows = self.run(
            "SELECT table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = ? ORDER BY table_name, ordinal_position",
            [schema],
        )
        return [{"table": r[0], "column": r[1], "type": r[2]} for r in rows]

    def table_exists(self, schema: str, table: str) -> bool:
        rows = self.run(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        )
        return bool(rows)
