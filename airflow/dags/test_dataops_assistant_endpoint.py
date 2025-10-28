from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests

def test_endpoint():
    response = requests.get("http://dataops-assistant:80")
    assert response.status_code == 200, f"Status code: {response.status_code}"
    print("Response:", response.text)
    print("DataOps Assistant endpoint is reachable.")

with DAG(
    dag_id="test_dataops_assistant_endpoint",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    test_task = PythonOperator(
        task_id="call_dataops_assistant",
        python_callable=test_endpoint,
    )
