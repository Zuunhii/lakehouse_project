from __future__ import annotations
import json, time
from io import BytesIO
from datetime import date, datetime
from decimal import Decimal

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests import Session
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ================== CONFIG ==================
HIVE_CATALOG = "minio"
BRONZE_SCHEMA = "temp"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "lakehouse"

# Nơi chứa Parquet nguồn (đã nạp sẵn từ MSSQL)
BRONZE_ROOT = "bronze/adventureworks"

# Prefix CHUNG chứa các bảng Iceberg (mỗi bảng 1 thư mục con riêng)
ICEBERG_PREFIX = "silver/iceberg"   # -> lakehouse/bronze/iceberg/<schema>/<table>/

TRINO_ENDPOINT = "http://trino-coordinator:8080/v1/statement"
TRINO_USER = "airflow"

ICEBERG_CATALOG = "iceberg"
ICEBERG_SCHEMA = "silver"  # schema trong catalog Iceberg

# Tuning
BATCH_SIZE = 1000          # số dòng mỗi batch INSERT
TRINO_TIMEOUT = 3600       # giây
POLL_SLEEP = 0.1           # giây giữa các lần poll nextUri
MAX_RETRY_POLL = 8         # retry khi poll nextUri bị rớt kết nối

# ================== UTILS ==================
SESSION: Session = Session()

def trino_sql(sql: str, timeout: int = TRINO_TIMEOUT,
              poll_sleep: float = POLL_SLEEP,
              max_retry: int = MAX_RETRY_POLL) -> dict:
    """Gửi SQL tới Trino, poll nextUri cho đến khi hoàn tất. Có retry khi poll bị rớt kết nối."""
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
                time.sleep(min(2 ** retries * 0.2, 5))  # backoff
        if "error" in payload:
            raise RuntimeError(json.dumps(payload["error"]))
        next_uri = payload.get("nextUri")

    return payload

def ident(x: str) -> str:
    """Quote identifier cho Trino."""
    return '"' + x.replace('"', '""') + '"'

# ---- Mapping kiểu dữ liệu ----
def arrow_to_trino(t: pa.DataType) -> str:
    # Integers
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t):
        return "integer"
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_uint8(t) or pa.types.is_uint16(t) or pa.types.is_uint32(t):
        return "integer"
    if pa.types.is_uint64(t):
        return "bigint"

    # Floats
    if pa.types.is_float16(t) or pa.types.is_float32(t):
        return "real"
    if pa.types.is_float64(t):
        return "double"

    # Boolean
    if pa.types.is_boolean(t):
        return "boolean"

    # Decimal
    if pa.types.is_decimal(t):
        return f"decimal({t.precision},{t.scale})"

    # Date / Timestamp
    if pa.types.is_date32(t) or pa.types.is_date64(t):
        return "date"
    if pa.types.is_timestamp(t):
        # Bạn có thể chi tiết tới timezone/unit nếu cần
        return "timestamp(6)"

    # Binary
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "varbinary"

    # String
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "varchar"

    # Complex (nâng cao): list -> array, struct -> row, map -> map
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        elem = arrow_to_trino(t.value_type)
        return f"array({elem})"
    if pa.types.is_struct(t):
        fields = ", ".join(f"{ident(f.name)} {arrow_to_trino(f.type)}" for f in t)
        return f"row({fields})"
    if pa.types.is_map(t):
        kt = arrow_to_trino(t.key_type)
        vt = arrow_to_trino(t.item_type)
        return f"map({kt}, {vt})"

    # Fallback an toàn
    return "varchar"

def to_sql_literal(v):
    """Chuyển Python value -> SQL literal Trino đúng kiểu."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, Decimal):
        return str(v)  # giữ nguyên scale
    if isinstance(v, date) and not isinstance(v, datetime):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, datetime):
        # nếu có tz -> chuyển về UTC và bỏ tz (Iceberg timestamp là "naive" theo Trino)
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        # format đủ 6 chữ số microseconds
        return f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    # string/others
    s = str(v).replace("'", "''")
    return f"'{s}'"

# ================== CORE TASK ==================
def register_single_table(schema_name: str, table_name: str):
    """
    1) Đọc schema của Parquet nguồn.
    2) Tạo bảng tạm trong Hive catalog với schema đó.
    3) Sử dụng CTAS để nạp dữ liệu từ bảng tạm vào bảng Iceberg.
    4) Xóa bảng tạm.
    """
    # 1) Key Parquet nguồn
    # This path points to the specific Parquet file.
    file_key = f"{BRONZE_ROOT}/{schema_name}/{table_name}/{table_name}.parquet"
    print(f"[INFO] Nguồn: s3://{BUCKET}/{file_key}")

    # 2) Đọc schema từ Parquet file
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    
    try:
        # Read the schema directly from the file to build the table DDL.
        obj = s3.get_object(Bucket=BUCKET, Key=file_key)
        parquet_file = pq.ParquetFile(BytesIO(obj["Body"].read()))
        schema_arrow = parquet_file.schema.to_arrow_schema()
    except Exception as e:
        print(f"[ERROR] Không đọc được schema từ Parquet: {e}")
        return

    # 3) Build DDL cột với mapping kiểu
    cols_sql = [f"{ident(f.name)} {arrow_to_trino(f.type)}" for f in schema_arrow]
    
    # 4) Tên bảng Iceberg & location
    iceberg_table_fqn = f'{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.silver_{schema_name.lower()}_{table_name.lower()}'
    table_path = f"s3a://{BUCKET}/{ICEBERG_PREFIX}/{schema_name}/{table_name}/"
    
    # 5) Tên bảng tạm trong Hive catalog
    hive_table_fqn = f'{HIVE_CATALOG}.{BRONZE_SCHEMA}.bronze_{schema_name.lower()}_{table_name.lower()}'
    
    # CORRECTED: The source path for the Hive table must be the PARENT DIRECTORY, not the file itself.
    source_dir = f"s3a://{BUCKET}/{BRONZE_ROOT}/{schema_name}/{table_name}/"
    
    print(f"[INFO] Bắt đầu xử lý: {iceberg_table_fqn}")

    # Bước A: Xóa bảng Iceberg cũ (nếu có)
    trino_sql(f"DROP TABLE IF EXISTS {iceberg_table_fqn}")

    # Bước B: Tạo một bảng tạm trong Hive Catalog trỏ tới thư mục nguồn
    print(f"[INFO] Tạo bảng tạm Hive {hive_table_fqn} trỏ tới {source_dir}")
    trino_sql(f"DROP TABLE IF EXISTS {hive_table_fqn}")
    
    create_hive_sql = (
        f"CREATE TABLE {hive_table_fqn} ({', '.join(cols_sql)}) "
        f"WITH (external_location = '{source_dir}', format = 'PARQUET')"
    )
    trino_sql(create_hive_sql)
    
    # Bước C: Sử dụng CTAS để nạp dữ liệu từ bảng tạm vào bảng Iceberg
    print(f"[INFO] Nạp dữ liệu vào Iceberg bằng CTAS từ {hive_table_fqn}")
    ctas_sql = (
        f"CREATE TABLE {iceberg_table_fqn} "
        f"WITH (location = '{table_path}') "
        f"AS SELECT * FROM {hive_table_fqn}"
    )
    trino_sql(ctas_sql)
    
    # Bước D: Dọn dẹp bảng tạm Hive
    print(f"[INFO] Xóa bảng tạm Hive: {hive_table_fqn}")
    trino_sql(f"DROP TABLE IF EXISTS {hive_table_fqn}")
    
    print(f"[DONE] Chèn xong dữ liệu vào {iceberg_table_fqn}")

# ================== DAG ==================
with DAG(
    dag_id="register_iceberg_single_table",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["bronze", "iceberg", "register", "ctas"],
) as dag:
    PythonOperator(
        task_id="register_table",
        python_callable=register_single_table,
        op_kwargs={
            "schema_name": "{{ dag_run.conf.get('schema') }}",
            "table_name": "{{ dag_run.conf.get('table') }}",
        },
    )