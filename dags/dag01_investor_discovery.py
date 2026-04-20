"""
DAG 1 — Investor discovery + raw crawl.

Reads investor websites from MySQL, upserts bronze_investors, fetches homepages via Jina,
writes bronze_web_fetches, detects portfolio URL candidates, updates silver_investor_pages.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from investor_etl.config import Settings
from investor_etl.stages import (
    ensure_schema_and_tables,
    stage1_detect_portfolio_candidates,
    stage1_fetch_homepages,
    stage1_mysql_to_bronze,
)

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}


def _settings() -> Settings:
    return Settings.from_env()


def task_ensure_tables():
    ensure_schema_and_tables(_settings())


def task_mysql_to_bronze():
    n = stage1_mysql_to_bronze(_settings())
    logger.info("Upserted %s bronze investor rows", n)


def task_fetch_homepages():
    n = stage1_fetch_homepages(_settings())
    logger.info("Fetched %s homepages", n)


def task_detect_portfolio_urls():
    n = stage1_detect_portfolio_candidates(_settings())
    logger.info("Inserted %s portfolio URL candidate rows", n)


with DAG(
    dag_id="investor_discovery_raw_crawl",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["investor", "bronze"],
) as dag:
    t0 = PythonOperator(task_id="ensure_schema_tables", python_callable=task_ensure_tables)
    t1 = PythonOperator(task_id="mysql_to_bronze_investors", python_callable=task_mysql_to_bronze)
    t2 = PythonOperator(task_id="fetch_homepages_jina", python_callable=task_fetch_homepages)
    t3 = PythonOperator(task_id="detect_portfolio_candidates", python_callable=task_detect_portfolio_urls)

    t0 >> t1 >> t2 >> t3
