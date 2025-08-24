from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"   # ĐÚNG trùng với volume bạn mount

with DAG(
    dag_id="dbt_bronze_to_silver",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["dbt","silver"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        set -e
        echo "PWD before: $(pwd)"
        ls -la {DBT_DIR}
        cd {DBT_DIR}
        if [ -f packages.yml ]; then
            echo "Found packages.yml -> running: dbt deps --profiles-dir ."
            dbt deps --profiles-dir .
        else
            echo "No packages.yml -> skip dbt deps (this is OK)"
        fi
        """
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        set -e
        cd {DBT_DIR}
        echo "== dbt debug =="
        dbt debug --profiles-dir . --target dev -v 2>&1
        echo "== dbt run =="
        dbt run --profiles-dir . --target dev --select models/silver -v 2>&1
        """
    )


dbt_deps >> dbt_run
