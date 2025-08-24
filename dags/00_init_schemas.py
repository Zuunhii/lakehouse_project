from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    "init_iceberg_schemas",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
) as dag:
    create = SQLExecuteQueryOperator(
        task_id="create_schemas",
        conn_id="trino_default",   # <- kết nối Trino trong Airflow
        sql=[
            "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location='s3a://lake/bronze/')",
            "CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location='s3a://lake/silver/')",
            "CREATE SCHEMA IF NOT EXISTS iceberg.gold   WITH (location='s3a://lake/gold/')",
        ],
    )
