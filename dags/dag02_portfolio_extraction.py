"""DAG 2 — Portfolio extraction via Jina + LLM into silver_investor_company_candidates."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from investor_etl.config import Settings
from investor_etl.stages import ensure_schema_and_tables, stage2_portfolio_extract

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "data-engineering", "depends_on_past": False, "retries": 1}


def task_prepare():
    ensure_schema_and_tables(Settings.from_env())


def task_extract():
    n = stage2_portfolio_extract(Settings.from_env())
    logger.info("Inserted %s silver candidate rows", n)


with DAG(
    dag_id="portfolio_extraction_llm",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["investor", "silver", "llm"],
) as dag:
    prepare = PythonOperator(task_id="ensure_tables", python_callable=task_prepare)
    extract = PythonOperator(task_id="portfolio_llm_extract", python_callable=task_extract)
    prepare >> extract
