"""
DAG: Crawl dữ liệu xe đạp → Bronze (Minio) → Silver (Iceberg)
FIXED: Chạy Scrapy như subprocess để tránh xung đột với Airflow
"""
from __future__ import annotations
import sys
import os
import time
import json
import subprocess
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests import Session

from airflow import DAG
from airflow.operators.python import PythonOperator

# ========== CONFIG ==========
BUCKET = "lakehouse"
BRONZE_ROOT = "bronze/bikes"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

HIVE_CATALOG = "minio"
HIVE_SCHEMA = "temp"

ICEBERG_CATALOG = "iceberg"
ICEBERG_SCHEMA = "silver"
ICEBERG_PREFIX = "silver/iceberg"

TRINO_ENDPOINT = "http://trino-coordinator:8080/v1/statement"
TRINO_USER = "airflow"

SESSION: Session = Session()

# ========== UTILS ==========
def trino_sql(sql: str, timeout: int = 600) -> dict:
    """Execute SQL trong Trino"""
    headers = {"X-Trino-User": TRINO_USER}
    r = SESSION.post(TRINO_ENDPOINT, data=sql.encode("utf-8"), headers=headers, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    
    if "error" in payload:
        raise RuntimeError(json.dumps(payload["error"]))
    
    next_uri = payload.get("nextUri")
    while next_uri:
        time.sleep(0.1)
        r = SESSION.get(next_uri, timeout=timeout)
        r.raise_for_status()
        payload = r.json()
        if "error" in payload:
            raise RuntimeError(json.dumps(payload["error"]))
        next_uri = payload.get("nextUri")
    
    return payload


def ident(x: str) -> str:
    return '"' + x.replace('"', '""') + '"'


# ========== TASKS ==========
def task_crawl_bikes(**context):
    """Task 1: Crawl data bằng subprocess"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    temp_file = f'/tmp/bikes_{timestamp}.json'
    
    # Path tới crawler script
    crawler_script = '/opt/airflow/crawlers/thongnhat/bike_crawler.py'
    
    print(f"[INFO] Starting crawler subprocess -> {temp_file}")
    
    # Chạy crawler như subprocess riêng biệt
    try:
        result = subprocess.run(
            ['python', crawler_script, temp_file],
            capture_output=True,
            text=True,
            timeout=600,  # 10 minutes timeout
            check=True
        )
        
        print(f"[STDOUT] {result.stdout}")
        if result.stderr:
            print(f"[STDERR] {result.stderr}")
        
        # Kiểm tra file đã được tạo chưa
        if not os.path.exists(temp_file):
            raise FileNotFoundError(f"Crawler không tạo được file: {temp_file}")
        
        # Kiểm tra file có data không
        with open(temp_file, 'r') as f:
            data = json.load(f)
            if not data:
                raise ValueError("File JSON rỗng!")
            print(f"[INFO] Crawled {len(data)} bikes successfully")
        
    except subprocess.TimeoutExpired:
        raise RuntimeError("Crawler timeout sau 10 phút!")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Crawler failed: {e.stderr}")
    
    # Push file path sang task tiếp
    context['ti'].xcom_push(key='json_file', value=temp_file)
    print(f"[DONE] Crawled to: {temp_file}")
    return temp_file


def task_json_to_parquet(**context):
    """Task 2: Convert JSON → Parquet và upload lên Minio"""
    ti = context['ti']
    json_file = ti.xcom_pull(task_ids='crawl_bikes', key='json_file')
    
    if not os.path.exists(json_file):
        raise FileNotFoundError(f"File not found: {json_file}")
    
    print(f"[INFO] Converting {json_file} to Parquet")
    
    # Đọc JSON
    df = pd.read_json(json_file, encoding='utf-8')
    
    if df.empty:
        raise ValueError("No data crawled!")
    
    print(f"[INFO] Crawled {len(df)} bikes")
    
    # Convert to Parquet
    table = pa.Table.from_pandas(df, preserve_index=False)
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer, compression='snappy')
    parquet_buffer.seek(0)
    
    # Upload lên Minio
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    
    s3_key = f"{BRONZE_ROOT}/thongnhat/thongnhat.parquet"
    
    s3.upload_fileobj(parquet_buffer, BUCKET, s3_key)
    print(f"[DONE] Uploaded to s3://{BUCKET}/{s3_key}")
    
    # Cleanup
    os.remove(json_file)
    
    ti.xcom_push(key='s3_key', value=s3_key)
    return s3_key


def task_register_to_iceberg(**context):
    """Task 3: Register Parquet → Iceberg Silver"""
    ti = context['ti']
    s3_key = ti.xcom_pull(task_ids='json_to_parquet', key='s3_key')
    
    print(f"[INFO] Registering {s3_key} to Iceberg")
    
    # Đọc schema từ Parquet
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    
    obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
    parquet_file = pq.ParquetFile(BytesIO(obj["Body"].read()))
    schema_arrow = parquet_file.schema.to_arrow_schema()
    
    # Build DDL
    def arrow_to_trino(t: pa.DataType) -> str:
        if pa.types.is_int32(t) or pa.types.is_int64(t): return "bigint"
        if pa.types.is_float64(t): return "double"
        if pa.types.is_boolean(t): return "boolean"
        if pa.types.is_timestamp(t): return "timestamp(6)"
        if pa.types.is_date32(t): return "date"
        return "varchar"
    
    cols_sql = [f"{ident(f.name)} {arrow_to_trino(f.type)}" for f in schema_arrow]
    
    # Tên bảng
    hive_tbl = f'{HIVE_CATALOG}.{HIVE_SCHEMA}.bronze_bikes_thongnhat'
    iceberg_tbl = f'{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.silver_bikes_thongnhat'
    
    source_dir = f"s3a://{BUCKET}/{BRONZE_ROOT}/thongnhat/"
    iceberg_location = f"s3a://{BUCKET}/{ICEBERG_PREFIX}/bikes/thongnhat/"
    
    print(f"[INFO] Creating Iceberg table: {iceberg_tbl}")
    
    # Drop old
    trino_sql(f"DROP TABLE IF EXISTS {iceberg_tbl}")
    trino_sql(f"DROP TABLE IF EXISTS {hive_tbl}")
    
    # Create temp Hive table
    trino_sql(f"CREATE SCHEMA IF NOT EXISTS {HIVE_CATALOG}.{HIVE_SCHEMA}")
    create_hive_sql = (
        f"CREATE TABLE {hive_tbl} ({', '.join(cols_sql)}) "
        f"WITH (external_location = '{source_dir}', format = 'PARQUET')"
    )
    trino_sql(create_hive_sql)
    
    # CTAS to Iceberg
    trino_sql(f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}")
    ctas_sql = (
        f"CREATE TABLE {iceberg_tbl} "
        f"WITH (location = '{iceberg_location}') "
        f"AS SELECT * FROM {hive_tbl}"
    )
    trino_sql(ctas_sql)
    
    # Drop temp
    trino_sql(f"DROP TABLE IF EXISTS {hive_tbl}")
    
    print(f"[DONE] Registered to {iceberg_tbl}")
    return iceberg_tbl


def task_verify_data(**context):
    """Task 4: Verify dữ liệu"""
    table = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.silver_bikes_thongnhat"
    
    sql = f"""
    SELECT 
        COUNT(*) as total_bikes,
        COUNT(DISTINCT tenxe) as unique_bikes,
        MAX(crawled_at) as latest_crawl
    FROM {table}
    """
    
    result = trino_sql(sql)
    print(f"[INFO] Verification result: {result}")
    
    if result.get('data'):
        row = result['data'][0]
        print(f"[STATS] Total bikes: {row[0]}, Unique: {row[1]}, Latest: {row[2]}")
    
    return "verified"


# ========== DAG ==========
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bike_crawl_to_lakehouse',
    default_args=default_args,
    description='Crawl bikes → Bronze → Silver (Fixed subprocess)',
    schedule_interval=None,
    catchup=False,
    tags=['crawler', 'bikes', 'bronze', 'silver'],
) as dag:

    t1 = PythonOperator(
        task_id='crawl_bikes',
        python_callable=task_crawl_bikes,
    )

    t2 = PythonOperator(
        task_id='json_to_parquet',
        python_callable=task_json_to_parquet,
    )

    t3 = PythonOperator(
        task_id='register_to_iceberg',
        python_callable=task_register_to_iceberg,
    )

    t4 = PythonOperator(
        task_id='verify_data',
        python_callable=task_verify_data,
    )

    t1 >> t2 >> t3 >> t4