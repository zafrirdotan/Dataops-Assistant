from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_dataops_pipeline',
    default_args=default_args,
    description='Run DataOps pipeline daily',
    schedule_interval='0 2 * * *',  # 2:00 AM UTC every day
    start_date=datetime(2025, 10, 19),
    catchup=False,
)

run_pipeline = BashOperator(
    task_id='run_pipeline',
    bash_command='curl -X POST http://dataops-assistant:80/trigger-pipeline',
    dag=dag,
)
