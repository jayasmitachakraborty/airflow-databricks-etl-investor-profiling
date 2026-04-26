"""Lightweight Databricks SQL Warehouse execution."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Optional

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


@contextmanager
def databricks_cursor(settings: Settings) -> Iterator[Any]:
    """
    Public cursor context manager.

    Use this to reuse a single warehouse session for many statements, which avoids
    repeated OpenSession/CloseSession overhead in tight loops.
    """
    with _cursor_conn(settings) as cur:
        yield cur


def execute_sql(settings: Settings, sql: str, *, cur: Optional[Any] = None) -> None:
    if cur is not None:
        cur.execute(sql)
        return
    with _cursor_conn(settings) as c:
        c.execute(sql)


def execute_many(
    settings: Settings, statements: Iterable[str], *, cur: Optional[Any] = None
) -> None:
    if cur is not None:
        for stmt in statements:
            cur.execute(stmt)
        return
    with _cursor_conn(settings) as c:
        for stmt in statements:
            c.execute(stmt)


def fetch_all(
    settings: Settings, sql: str, *, cur: Optional[Any] = None
) -> list[tuple[Any, ...]]:
    if cur is not None:
        cur.execute(sql)
        return cur.fetchall()
    with _cursor_conn(settings) as c:
        c.execute(sql)
        return c.fetchall()


def fetch_scalar(settings: Settings, sql: str) -> Any:
    rows = fetch_all(settings, sql)
    if not rows:
        return None
    return rows[0][0]
