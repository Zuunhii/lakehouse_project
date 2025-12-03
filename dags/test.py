from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.hooks.trino import TrinoHook


def get_trino_hook():
    return TrinoHook(
        host="trino-coordinator",
        port=8080,
        http_scheme="http",
        catalog="system",
        schema="information_schema",
        user="airflow",
    )


def test_trino():
    hook = get_trino_hook()
    # Chạy SELECT 1
    rows = hook.get_records("SELECT 1")
    print("Trino result:", rows)


with DAG(
    dag_id="test_trino_connection",
    start_date=datetime(2025, 11, 18),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_conn = PythonOperator(
        task_id="test_trino",
        python_callable=test_trino,
    )
