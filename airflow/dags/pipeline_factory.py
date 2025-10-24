from datetime import datetime, timedelta
from pathlib import Path
import json

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

CATALOG_PATH = Path("/opt/airflow/dags/pipelines/catalog.json")

def make_dag(p: dict) -> DAG:
    default_args = {
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    dag = DAG(
        dag_id=p['id'],
        description=p.get("description", ""),
        schedule=p.get("schedule"),            # Airflow 3.x uses `schedule`
        start_date=datetime.fromisoformat(p.get("start_date")) if p.get("start_date") else datetime.now(),
        catchup=False,
        default_args=default_args,
        tags=p.get("tags", ["pipeline"]),
        max_active_runs=1,                     # optional: prevent overlap per pipeline
    )

    trigger_backend = HttpOperator(
        task_id=f"trigger_backend_{p['id']}",
        http_conn_id="dataops_assistant",  # Defined in Airflow Connections
        endpoint=f"/trigger-pipeline?pipeline_id={p['id']}",
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        log_response=True,
        response_check=lambda response: response.status_code in (200, 202),
        retries=1,
        dag=dag,
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
