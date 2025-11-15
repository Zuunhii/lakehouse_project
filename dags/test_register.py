from __future__ import annotations
import json, time
from io import BytesIO
from datetime import date, datetime, timezone
from decimal import Decimal

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from requests import Session
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook

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
ICEBERG_PREFIX = "silver/iceberg"   # -> lakehouse/silver/iceberg/<schema>/<table>/

ICEBERG_CATALOG = "iceberg"
ICEBERG_SCHEMA = "silver"  # schema trong catalog Iceberg

# Tuning
BATCH_SIZE = 1000          # số dòng mỗi batch INSERT
TRINO_TIMEOUT = 3600       # giây
POLL_SLEEP = 0.1           # giây giữa các lần poll nextUri
MAX_RETRY_POLL = 8         # retry khi poll nextUri bị rớt kết nối

# ================== TRINO SESSION VIA AIRFLOW CONNECTION ==================
def get_trino_session(conn_id: str = "trino_default") -> Session:
    """
    Lấy thông tin kết nối từ Airflow Connection:
    - host/port
    - login/password (LDAP)
    - extra: {"http_scheme":"https","verify": false | "/opt/certs/trino-cert.pem"}
    """
    conn = BaseHook.get_connection(conn_id)

    sess = Session()

    # verify: false (vá tạm) hoặc path tới PEM nếu đã mount
    extra = conn.extra_dejson or {}
    verify_val = extra.get("verify", True)
    sess.verify = verify_val

    # auth: Basic LDAP
    if conn.login and conn.password:
        sess.auth = (conn.login, conn.password)

    # scheme + endpoint
    scheme = extra.get("http_scheme", "https")
    host = conn.host or "trino-coordinator"
    port = conn.port or (8443 if scheme == "https" else 8080)
    sess.trino_endpoint = f"{scheme}://{host}:{port}/v1/statement"

    # headers mặc định
    sess.headers.update({
        "X-Trino-User": conn.login or "airflow",
        "X-Trino-Source": "airflow-register",
    })
    return sess

def trino_sql(sql: str,
              timeout: int = TRINO_TIMEOUT,
              poll_sleep: float = POLL_SLEEP,
              max_retry: int = MAX_RETRY_POLL,
              conn_id: str = "trino_default") -> dict:
    """Gửi SQL tới Trino qua REST API /v1/statement, poll nextUri cho đến khi hoàn tất."""
    sess = get_trino_session(conn_id)

    r = sess.post(sess.trino_endpoint, data=sql.encode("utf-8"),
                  headers=sess.headers, timeout=timeout)
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
                r = sess.get(next_uri, headers=sess.headers, timeout=timeout)
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

# ================== HELPERS ==================
def ident(x: str) -> str:
    """Quote identifier cho Trino."""
    return '"' + x.replace('"', '""') + '"'

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
        return "timestamp(6)"
    # Binary
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "varbinary"
    # String
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "varchar"
    # Complex
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
    # Fallback
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
        return str(v)
    if isinstance(v, date) and not isinstance(v, datetime):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    s = str(v).replace("'", "''")
    return f"'{s}'"

# ================== CORE TASK ==================
def register_single_table(schema_name: str, table_name: str, trino_conn_id: str = "trino_default"):
    """
    1) Đọc schema của Parquet nguồn.
    2) Tạo bảng tạm trong Hive catalog với schema đó.
    3) CTAS nạp dữ liệu vào bảng Iceberg.
    4) Dọn dẹp.
    """
    file_key = f"{BRONZE_ROOT}/{schema_name}/{table_name}/{table_name}.parquet"
    print(f"[INFO] Nguồn: s3://{BUCKET}/{file_key}")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=file_key)
        parquet_file = pq.ParquetFile(BytesIO(obj["Body"].read()))
        schema_arrow = parquet_file.schema.to_arrow_schema()
    except Exception as e:
        print(f"[ERROR] Không đọc được schema từ Parquet: {e}")
        return

    cols_sql = [f"{ident(f.name)} {arrow_to_trino(f.type)}" for f in schema_arrow]

    iceberg_table_fqn = f'{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.silver_{schema_name.lower()}_{table_name.lower()}'
    table_path = f"s3a://{BUCKET}/{ICEBERG_PREFIX}/{schema_name}/{table_name}/"

    hive_table_fqn = f'{HIVE_CATALOG}.{BRONZE_SCHEMA}.bronze_{schema_name.lower()}_{table_name.lower()}'
    source_dir = f"s3a://{BUCKET}/{BRONZE_ROOT}/{schema_name}/{table_name}/"

    print(f"[INFO] Bắt đầu xử lý: {iceberg_table_fqn}")

    # A) Drop Iceberg cũ
    trino_sql(f"DROP TABLE IF EXISTS {iceberg_table_fqn}", conn_id=trino_conn_id)

    # B) Tạo bảng tạm Hive trỏ tới thư mục parquet
    print(f"[INFO] Tạo bảng tạm Hive {hive_table_fqn} trỏ tới {source_dir}")
    trino_sql(f"DROP TABLE IF EXISTS {hive_table_fqn}", conn_id=trino_conn_id)

    create_hive_sql = (
        f"CREATE TABLE {hive_table_fqn} ({', '.join(cols_sql)}) "
        f"WITH (external_location = '{source_dir}', format = 'PARQUET')"
    )
    trino_sql(create_hive_sql, conn_id=trino_conn_id)

    # C) CTAS sang Iceberg
    print(f"[INFO] Nạp dữ liệu vào Iceberg bằng CTAS từ {hive_table_fqn}")
    ctas_sql = (
        f"CREATE TABLE {iceberg_table_fqn} "
        f"WITH (location = '{table_path}') "
        f"AS SELECT * FROM {hive_table_fqn}"
    )
    trino_sql(ctas_sql, conn_id=trino_conn_id)

    # D) Dọn dẹp
    print(f"[INFO] Xóa bảng tạm Hive: {hive_table_fqn}")
    trino_sql(f"DROP TABLE IF EXISTS {hive_table_fqn}", conn_id=trino_conn_id)

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
            "trino_conn_id": "{{ dag_run.conf.get('trino_conn_id', 'trino_default') }}",
        },
    )
