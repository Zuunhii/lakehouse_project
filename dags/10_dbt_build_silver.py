from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_DIR = "/opt/airflow/dags/dbt_project"

with DAG("dbt_build_silver", start_date=datetime(2025,8,1), schedule="@hourly", catchup=False) as dag:
    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps || true"
    )
    build = BashOperator(
        task_id="dbt_build_silver",
        bash_command=f"cd {DBT_DIR} && dbt build --select models/silver"
    )
    deps >> build
