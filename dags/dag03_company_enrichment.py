"""DAG 3 — Dedupe company sites, crawl via Jina, populate silver_companies + investor map."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from investor_etl.config import Settings
from investor_etl.stages import ensure_schema_and_tables, stage3_company_enrichment

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "data-engineering", "depends_on_past": False, "retries": 1}


def task_prepare():
    ensure_schema_and_tables(Settings.from_env())


def task_enrich():
    n = stage3_company_enrichment(Settings.from_env())
    logger.info("Processed %s investor-company links", n)


with DAG(
    dag_id="03_company_enrichment",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["investor", "silver", "jina"],
) as dag:
    prepare = PythonOperator(task_id="ensure_tables", python_callable=task_prepare)
    enrich = PythonOperator(task_id="dedupe_and_crawl_companies", python_callable=task_enrich)
    prepare >> enrich
