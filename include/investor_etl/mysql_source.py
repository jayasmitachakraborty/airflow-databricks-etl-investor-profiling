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


@dataclass(frozen=True)
class TaxonomyRow:
    theme: str
    main_category: str
    subcategory: str


def _mysql_connect(settings: Settings):
    try:
        import mysql.connector
    except ImportError as e:
        raise RuntimeError("Install mysql-connector-python for MySQL ingestion.") from e

    return mysql.connector.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
    )


def iter_investors(settings: Settings) -> list[InvestorRow]:
    conn = _mysql_connect(settings)
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


def fetch_taxonomy_rows(settings: Settings) -> list[TaxonomyRow]:
    """
    Loads ground-truth taxonomy from MySQL.

    Expected table: `noa_taxonomy.taxonomy_list` with columns:
      - theme
      - main_Category
      - Subcategory
    """
    conn = _mysql_connect(settings)
    rows: list[TaxonomyRow] = []
    try:
        cur = conn.cursor(dictionary=True)
        db = settings.mysql_taxonomy_database
        cur.execute(
            f"""
SELECT
  `theme` AS theme,
  `main_Category` AS main_category,
  `Subcategory` AS subcategory
FROM `{db}`.`taxonomy_list`
WHERE `theme` IS NOT NULL
  AND `main_Category` IS NOT NULL
  AND `Subcategory` IS NOT NULL
"""
        )
        for r in cur.fetchall():
            rows.append(
                TaxonomyRow(
                    theme=str(r["theme"]).strip(),
                    main_category=str(r["main_category"]).strip(),
                    subcategory=str(r["subcategory"]).strip(),
                )
            )
        cur.close()
    finally:
        conn.close()
    return rows
