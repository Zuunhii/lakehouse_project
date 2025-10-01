from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="demo_trigger_with_config",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    params={"run_mode": "full", "limit_rows": 1000},  # giúp form w/ config xuất hiện rõ ràng
) as dag:
    EmptyOperator(task_id="start")
