"""DAG 4 — Hierarchical LLM classification: built world gate, then theme → main category → subcategory (dbt marts downstream)."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from investor_etl.config import Settings
from investor_etl.stages import ensure_schema_and_tables, stage4_classify

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "data-engineering", "depends_on_past": False, "retries": 1}


def task_prepare():
    ensure_schema_and_tables(Settings.from_env())


def task_classify():
    n = stage4_classify(Settings.from_env())
    logger.info("Classified %s companies", n)


with DAG(
    dag_id="company_classification",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["investor", "silver", "llm", "classification"],
) as dag:
    prepare = PythonOperator(task_id="ensure_tables", python_callable=task_prepare)
    classify = PythonOperator(
        task_id="hierarchical_llm_classify",
        python_callable=task_classify,
    )
    prepare >> classify
