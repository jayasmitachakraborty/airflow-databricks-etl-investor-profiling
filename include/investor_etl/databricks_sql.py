"""Lightweight Databricks SQL Warehouse execution."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterable

from investor_etl.config import Settings


@contextmanager
def _cursor_conn(settings: Settings):
    try:
        from databricks import sql as dsql
    except ImportError as e:
        raise RuntimeError(
            "Install databricks-sql-connector for warehouse execution."
        ) from e

    conn = dsql.connect(
        server_hostname=settings.databricks_host,
        http_path=settings.databricks_http_path,
        access_token=settings.databricks_token,
    )
    try:
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()
    finally:
        conn.close()


def execute_sql(settings: Settings, sql: str) -> None:
    with _cursor_conn(settings) as cur:
        cur.execute(sql)


def execute_many(settings: Settings, statements: Iterable[str]) -> None:
    with _cursor_conn(settings) as cur:
        for stmt in statements:
            cur.execute(stmt)


def fetch_all(settings: Settings, sql: str) -> list[tuple[Any, ...]]:
    with _cursor_conn(settings) as cur:
        cur.execute(sql)
        return cur.fetchall()


def fetch_scalar(settings: Settings, sql: str) -> Any:
    rows = fetch_all(settings, sql)
    if not rows:
        return None
    return rows[0][0]
