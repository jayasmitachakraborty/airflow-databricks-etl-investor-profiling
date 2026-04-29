"""Lightweight Databricks SQL Warehouse execution.

This module is intentionally small, but we add a thin resiliency layer: the SQL
warehouse / thrift session can occasionally drop mid-poll, which surfaces as
errors like:

- RemoteDisconnected('Remote end closed connection without response')
- RESOURCE_DOES_NOT_EXIST: Command <id> does not exist.

Those errors are usually transient and safe to retry by opening a new session
and re-executing the statement.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Optional


def _is_transient_databricks_error(exc: BaseException) -> bool:
    msg = str(exc)
    needles = (
        "RemoteDisconnected",
        "remote end closed connection",
        "connection broken",
        "RESOURCE_DOES_NOT_EXIST: Command",
        "does not exist",
        "BadStatusLine",
        "EOF occurred in violation of protocol",
    )
    return any(n in msg for n in needles)


class _ResilientCursor:
    """
    Wraps a Databricks SQL cursor and recreates the session once on transient errors.
    """

    def __init__(self, settings: "Settings"):
        self._settings = settings
        self._conn: Any | None = None
        self._cur: Any | None = None
        self._open()

    def _open(self) -> None:
        from databricks import sql as dsql

        self._conn = dsql.connect(
            server_hostname=self._settings.databricks_host,
            http_path=self._settings.databricks_http_path,
            access_token=self._settings.databricks_token,
        )
        self._cur = self._conn.cursor()

    def _close_inner(self) -> None:
        cur, conn = self._cur, self._conn
        self._cur, self._conn = None, None
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def close(self) -> None:
        self._close_inner()

    def execute(self, sql: str) -> Any:
        assert self._cur is not None
        try:
            return self._cur.execute(sql)
        except Exception as exc:
            if not _is_transient_databricks_error(exc):
                raise
            # Reopen session and retry once.
            self._close_inner()
            self._open()
            assert self._cur is not None
            return self._cur.execute(sql)

    def fetchall(self) -> Any:
        assert self._cur is not None
        return self._cur.fetchall()

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
    cur = _ResilientCursor(settings)
    try:
        yield cur
    finally:
        cur.close()


def execute_sql(settings: Settings, sql: str, *, cur: Optional[Any] = None) -> None:
    if cur is not None:
        cur.execute(sql)
        return
    # One-off execution still gets a single retry on transient errors.
    c = _ResilientCursor(settings)
    try:
        c.execute(sql)
    finally:
        c.close()


def execute_many(
    settings: Settings, statements: Iterable[str], *, cur: Optional[Any] = None
) -> None:
    if cur is not None:
        for stmt in statements:
            cur.execute(stmt)
        return
    c = _ResilientCursor(settings)
    try:
        for stmt in statements:
            c.execute(stmt)
    finally:
        c.close()


def fetch_all(
    settings: Settings, sql: str, *, cur: Optional[Any] = None
) -> list[tuple[Any, ...]]:
    if cur is not None:
        cur.execute(sql)
        return cur.fetchall()
    c = _ResilientCursor(settings)
    try:
        c.execute(sql)
        return c.fetchall()
    finally:
        c.close()


def fetch_scalar(settings: Settings, sql: str) -> Any:
    rows = fetch_all(settings, sql)
    if not rows:
        return None
    return rows[0][0]
