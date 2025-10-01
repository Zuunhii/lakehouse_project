from __future__ import annotations
import time, json

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ==== CONFIG ====
TRINO = "http://trino-coordinator:8080/v1/statement"
TRINO_USER = "airflow"

ICEBERG_CATALOG = "iceberg"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

def trino_sql(sql: str, timeout=600) -> dict:
    headers = {"X-Trino-User": TRINO_USER}
    r = requests.post(TRINO, data=sql.encode("utf-8"), headers=headers, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    while "nextUri" in payload:
        time.sleep(0.05)
        r = requests.get(payload["nextUri"], timeout=timeout)
        r.raise_for_status()
        payload = r.json()
    if "error" in payload:
        raise RuntimeError(json.dumps(payload["error"]))
    return payload

def ctas_to_silver(source_schema, source_table, target_table):
    trino_sql(f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{SILVER_SCHEMA}")
    
    source_full_table = f"{ICEBERG_CATALOG}.{BRONZE_SCHEMA}.bronze_{source_schema}_{source_table}"
    target_full_table = f"{ICEBERG_CATALOG}.{SILVER_SCHEMA}.silver_{target_table}"

    print(f"Đang xử lý bảng: {source_full_table} -> {target_full_table}")
    
    # Ví dụ: Giả sử bạn muốn làm sạch dữ liệu bằng cách loại bỏ các bản ghi trùng lặp
    ctas_sql = f"""
        CREATE OR REPLACE TABLE {target_full_table} WITH (
            format = 'PARQUET'
        ) AS
        SELECT *
        FROM {source_full_table}
        -- WHERE ... (Thêm các điều kiện làm sạch dữ liệu ở đây)
    """
    trino_sql(ctas_sql)
    print(f"  Đã tạo bảng Silver: {target_full_table} từ {source_full_table}")

with DAG(
    dag_id="ctas_to_silver",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["silver", "iceberg", "ctas"],
) as dag:
    ctas_task = PythonOperator(
        task_id="ctas_table",
        python_callable=ctas_to_silver,
        op_kwargs={
            "source_schema": "{{ dag_run.conf['schema'] }}",
            "source_table": "{{ dag_run.conf['table'] }}",
            "target_table": "{{ dag_run.conf['table'] }}"
        }
    )