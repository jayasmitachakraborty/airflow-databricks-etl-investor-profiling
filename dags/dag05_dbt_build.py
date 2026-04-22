"""DAG 5 — dbt source freshness, build, and mart publication."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from investor_etl.stages import stage5_dbt

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "data-engineering", "depends_on_past": False, "retries": 1}

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DBT = ROOT / "dbt"


def task_dbt():
    project = os.getenv("DBT_PROJECT_DIR", str(DEFAULT_DBT))
    profiles = os.getenv("DBT_PROFILES_DIR")
    stage5_dbt(project, profiles)


with DAG(
    dag_id="05_dbt_build_marts",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["investor", "dbt", "marts"],
) as dag:
    PythonOperator(task_id="dbt_freshness_and_build", python_callable=task_dbt)
