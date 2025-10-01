# airflow >=2.5
from __future__ import annotations
import json, time
from io import BytesIO
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import requests
from requests import Session
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ================== CONFIG ==================
# Parquet nguồn đã dump từ MSSQL
BUCKET = "lakehouse"
BRONZE_ROOT = "bronze/adventureworks"   # s3://lakehouse/bronze/adventureworks/<Schema>/<Table>/<Table>.parquet

# Danh sách BẢNG CẦN ĐĂNG KÝ (tuần tự)
TABLES_TO_REGISTER = [
    ("Person", "Address"),
    ("Person", "CountryRegion"),
    ("Person", "Person"),
    ("Person", "StateProvince"),
    ("Production", "ProductSubcategory"),
    ("Production","Product"),
    ("Production", "ProductCategory"),
    ("Sales", "CreditCard"),
    ("Sales", "Customer"),
    ("Sales", "SalesOrderDetail"),
    ("Sales", "SalesOrderHeader"),
    ("Sales", "SalesOrderHeaderSalesReason"),
    ("Sales", "SalesReason"),
    ("Sales", "Store"),
]

# MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

# Hive catalog tạm để đọc trực tiếp thư mục Parquet
HIVE_CATALOG = "minio"
HIVE_SCHEMA   = "temp"

# Iceberg đích
ICEBERG_CATALOG = "iceberg"
ICEBERG_SCHEMA  = "silver"            # -> iceberg.silver.silver_<schema>_<table>
ICEBERG_PREFIX  = "silver/iceberg"    # -> s3a://lakehouse/silver/iceberg/<Schema>/<Table>/

# Trino
TRINO_ENDPOINT = "http://trino-coordinator:8080/v1/statement"
TRINO_USER = "airflow"
TRINO_TIMEOUT = 3600
POLL_SLEEP = 0.1
MAX_RETRY_POLL = 8

# dbt
DBT_DIR = "/opt/airflow/dbt"
DBT_TARGET = "dev"

# ================== UTILS ==================
SESSION: Session = Session()

def trino_sql(sql: str, timeout: int = TRINO_TIMEOUT,
              poll_sleep: float = POLL_SLEEP,
              max_retry: int = MAX_RETRY_POLL) -> dict:
    headers = {"X-Trino-User": TRINO_USER}
    r = SESSION.post(TRINO_ENDPOINT, data=sql.encode("utf-8"), headers=headers, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    if "error" in payload:
        raise RuntimeError(json.dumps(payload["error"]))

    next_uri = payload.get("nextUri")
    while next_uri:
        retries = 0
        while True:
            try:
                time.sleep(poll_sleep)
                r = SESSION.get(next_uri, timeout=timeout)
                r.raise_for_status()
                payload = r.json()
                break
            except (ConnectionError, ProtocolError) as e:
                retries += 1
                if retries > max_retry:
                    raise RuntimeError(f"Polling failed after {max_retry} retries: {e}")
                time.sleep(min(2 ** retries * 0.2, 5))
        if "error" in payload:
            raise RuntimeError(json.dumps(payload["error"]))
        next_uri = payload.get("nextUri")
    return payload

def ident(x: str) -> str:
    return '"' + x.replace('"', '""') + '"'

def arrow_to_trino(t: pa.DataType) -> str:
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t): return "integer"
    if pa.types.is_int64(t): return "bigint"
    if pa.types.is_uint8(t) or pa.types.is_uint16(t) or pa.types.is_uint32(t): return "integer"
    if pa.types.is_uint64(t): return "bigint"
    if pa.types.is_float16(t) or pa.types.is_float32(t): return "real"
    if pa.types.is_float64(t): return "double"
    if pa.types.is_boolean(t): return "boolean"
    if pa.types.is_decimal(t): return f"decimal({t.precision},{t.scale})"
    if pa.types.is_date32(t) or pa.types.is_date64(t): return "date"
    if pa.types.is_timestamp(t): return "timestamp(6)"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t): return "varbinary"
    if pa.types.is_string(t) or pa.types.is_large_string(t): return "varchar"
    if pa.types.is_list(t) or pa.types.is_large_list(t): return f"array({arrow_to_trino(t.value_type)})"
    if pa.types.is_struct(t):
        fields = ", ".join(f"{ident(f.name)} {arrow_to_trino(f.type)}" for f in t)
        return f"row({fields})"
    if pa.types.is_map(t): return f"map({arrow_to_trino(t.key_type)}, {arrow_to_trino(t.item_type)})"
    return "varchar"

def register_single_table(schema_name: str, table_name: str):
    # 1) xác định nguồn parquet
    file_key = f"{BRONZE_ROOT}/{schema_name}/{table_name}/{table_name}.parquet"
    print(f"[INFO] Source Parquet: s3://{BUCKET}/{file_key}")

    # 2) đọc schema parquet
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    obj = s3.get_object(Bucket=BUCKET, Key=file_key)
    parquet_file = pq.ParquetFile(BytesIO(obj["Body"].read()))
    schema_arrow = parquet_file.schema.to_arrow_schema()
    cols_sql = [f"{ident(f.name)} {arrow_to_trino(f.type)}" for f in schema_arrow]

    # 3) fqn các bảng
    hive_tbl = f'{HIVE_CATALOG}.{HIVE_SCHEMA}.bronze_{schema_name.lower()}_{table_name.lower()}'
    iceberg_tbl = f'{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.silver_{schema_name.lower()}_{table_name.lower()}'
    src_dir = f"s3a://{BUCKET}/{BRONZE_ROOT}/{schema_name}/{table_name}/"
    dst_dir = f"s3a://{BUCKET}/{ICEBERG_PREFIX}/{schema_name}/{table_name}/"

    print(f"[INFO] Register -> {iceberg_tbl}")
    # dọn cũ
    trino_sql(f"DROP TABLE IF EXISTS {iceberg_tbl}")
    trino_sql(f"DROP TABLE IF EXISTS {hive_tbl}")

    # tạo bảng tạm hive trỏ thư mục nguồn
    trino_sql(f"CREATE SCHEMA IF NOT EXISTS {ident(HIVE_CATALOG)}.{ident(HIVE_SCHEMA)}")
    create_hive_sql = (
        f"CREATE TABLE {hive_tbl} ({', '.join(cols_sql)}) "
        f"WITH (external_location = '{src_dir}', format = 'PARQUET')"
    )
    trino_sql(create_hive_sql)

    # CTAS vào Iceberg
    trino_sql(f"CREATE SCHEMA IF NOT EXISTS {ident(ICEBERG_CATALOG)}.{ident(ICEBERG_SCHEMA)}")
    ctas_sql = (
        f"CREATE TABLE {iceberg_tbl} WITH (location = '{dst_dir}') "
        f"AS SELECT * FROM {hive_tbl}"
    )
    trino_sql(ctas_sql)

    # drop bảng tạm
    trino_sql(f"DROP TABLE IF EXISTS {hive_tbl}")
    print(f"[DONE] {iceberg_tbl}")

# ================== TASK FUNCS ==================
def extract_full_db():
    # gọi lại function extract hiện có của bạn (module cũ)
    from AdventureWorks2022_to_Minio_Full import extract_full_db as _run
    _run()

def register_selected_tables_sequential():
    """Chạy tuần tự qua TABLES_TO_REGISTER, có retry nhẹ giữa các bảng."""
    for i, (schema, table) in enumerate(TABLES_TO_REGISTER, start=1):
        tries = 0
        while True:
            try:
                print(f"\n=== [{i}/{len(TABLES_TO_REGISTER)}] {schema}.{table} ===")
                register_single_table(schema, table)
                # nghỉ 1 chút cho Trino/HMS thở
                time.sleep(1.0)
                break
            except Exception as e:
                tries += 1
                print(f"[WARN] Failed {schema}.{table} (try {tries}): {e}")
                if tries >= 3:
                    raise
                time.sleep(min(5 * tries, 20))

# ================== DAG ==================
with DAG(
    dag_id="e2e_adventureworks_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,          # bật lịch tuỳ bạn
    catchup=False,
    tags=["e2e","bronze","silver","gold","dbt"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_full_db",
        python_callable=extract_full_db,
        retries=0,
    )

    t_register_seq = PythonOperator(
        task_id="register_iceberg_silver",
        python_callable=register_selected_tables_sequential,
        retries=1,                    # retry toàn job nếu vẫn lỗi
        retry_delay=timedelta(minutes=1),
    )

    dbt_run = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            'set -euo pipefail; '
            'export PATH="$PATH:/home/airflow/.local/bin"; '  # << thêm dòng này
            f'cd {DBT_DIR}; '
            'dbt deps -v; '
            f'dbt debug --profiles-dir . --target {DBT_TARGET} -v; '
            # build models gắn tag:gold (dim + fact + OBT), bạn đã gắn tag trong dbt_project.yml
            f'dbt run  --profiles-dir . --target {DBT_TARGET} --select tag:gold; '
            f'dbt test --profiles-dir . --target {DBT_TARGET} --select tag:gold; '
        ),
        env={"DBT_PROFILES_DIR": DBT_DIR, "DBT_LOG_LEVEL":"debug","DBT_LOG_FORMAT":"text"},
        retries=0,
    )

    # thứ tự
    t_extract >> t_register_seq >> dbt_run
