from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
import boto3
from io import BytesIO
import uuid


# Config MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "lakehouse"


def extract_and_load(**context):
    # Kết nối tới SQL Server
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    sql = "USE AdventureWorks2022; SELECT TOP 100 * FROM Person.Address"   # Truy vấn thử
    df = hook.get_pandas_df(sql)

    # Tự động chuyển đổi các cột có kiểu dữ liệu không tương thích thành chuỗi (string) với xử lý lỗi
    for col in df.columns:
        if df[col].dtype == "object" or isinstance(df[col].iloc[0], uuid.UUID):
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, str) or isinstance(x, uuid.UUID) else str(x).encode('utf-8', 'ignore').decode('utf-8'))

    # Convert sang Parquet (thô, bronze layer)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Upload lên MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    key = "bronze/adventureworks/person_address.parquet"
    s3.upload_fileobj(buffer, BUCKET_NAME, key)
    print(f"Uploaded {key} to MinIO")

with DAG(
    dag_id="mssql_to_minio",
    start_date=days_ago(1),
    schedule_interval=None,  # chạy tay
    catchup=False,
    tags=["example"],
) as dag:

    task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
        provide_context=True,
    )
