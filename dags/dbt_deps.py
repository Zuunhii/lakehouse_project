from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_deps",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt","deps"],
) as dag:
    BashOperator(
        task_id="dbt_deps",
        bash_command=(
            'set -xeuo pipefail; '
            'export PATH="$PATH:/home/airflow/.local/bin"; '
            f'cd {DBT_DIR}; '
            'echo "== PWD ==" && pwd; '
            'echo "== LIST ==" && ls -la; '
            'echo "== packages.yml ==" && sed -n "1,200p" packages.yml; '
            'echo "== dbt deps -v =="; '
            'dbt deps -v'
        ),
        env={"DBT_PROFILES_DIR": DBT_DIR},
    )
