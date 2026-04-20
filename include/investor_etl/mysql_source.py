"""Read investor seed rows from MySQL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from investor_etl.config import Settings


@dataclass(frozen=True)
class InvestorRow:
    investor_id: str
    investor_name: str
    source_website: str


def iter_investors(settings: Settings) -> list[InvestorRow]:
    try:
        import mysql.connector
    except ImportError as e:
        raise RuntimeError("Install mysql-connector-python for MySQL ingestion.") from e

    conn = mysql.connector.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
    )
    rows: list[InvestorRow] = []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(settings.mysql_investors_query)
        for r in cur.fetchall():
            rows.append(
                InvestorRow(
                    investor_id=str(r["investor_id"]),
                    investor_name=str(r["investor_name"]),
                    source_website=str(r["source_website"]),
                )
            )
        cur.close()
    finally:
        conn.close()
    return rows


def fetch_investors_dicts(settings: Settings) -> list[dict[str, Any]]:
    return [
        {
            "investor_id": x.investor_id,
            "investor_name": x.investor_name,
            "source_website": x.source_website,
        }
        for x in iter_investors(settings)
    ]
