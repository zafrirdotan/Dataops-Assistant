from datetime import datetime, timedelta
from pathlib import Path
import json

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

CATALOG_PATH = Path("/opt/airflow/dags/pipelines/catalog.json")

def make_dag(p: dict) -> DAG:
    default_args = {
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    dag = DAG(
        dag_id=f"pipeline_{p['id']}",
        description=p.get("description", ""),
        schedule=p.get("schedule"),            # Airflow 3.x uses `schedule`
        start_date=datetime(2025, 10, 19),
        catchup=False,
        default_args=default_args,
        tags=p.get("tags", ["pipeline"]),
        max_active_runs=1,                     # optional: prevent overlap per pipeline
    )

    BashOperator(
        task_id="run_backend",
        bash_command=(
            "curl -sS -X POST http://dataops-assistant:80/run "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"pipeline_id\": \"{p['id']}\", \"mode\": \"{p.get('mode','full')}\"}}'"
        ),
        dag=dag,
        do_xcom_push=False,
        retries=p.get("retries", 1),
    )

    return dag

# Build all DAGs at import time (FAST: local file read only)
if CATALOG_PATH.exists():
    try:
        data = json.loads(CATALOG_PATH.read_text() or "{}")
        for pipe in data.get("pipelines", []):
            globals()[f"pipeline_{pipe['id']}"] = make_dag(pipe)
    except Exception as e:
        # Parsing errors should surface in Airflow UI; keeping it minimal here
        raise
