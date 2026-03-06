from __future__ import annotations

from datetime import timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import urllib3
from requests import Session

import pendulum
VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

urllib3.disable_warnings()

# =========================================
# CONFIG
# =========================================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "lakehouse"

# Nơi chứa dữ liệu fake hằng ngày
FAKE_PREFIX = "bronze/adventureworks/Sales/Faker_SalesOrderDetail_daily"

# Bảng Iceberg đích ở bronze (đổi đúng bảng LIVE của mày)
ICEBERG_TABLE = "iceberg.bronze.bronze_sales_salesorderdetail_LIVE"

# Hive catalog/schema dùng làm bảng tạm trỏ vào Parquet
HIVE_CATALOG = "minio"
HIVE_SCHEMA = "temp"

CHECK_INTERVAL_SECS = 5 * 60  # 5 phút
TRINO_CONN_ID = "trino_default"


# =========================================
# TRINO REST API (GOM HẾT DATA)
# =========================================
def get_trino_session(conn_id: str = TRINO_CONN_ID) -> Session:
    conn = BaseHook.get_connection(conn_id)
    sess = Session()

    extra = conn.extra_dejson or {}
    sess.verify = extra.get("verify", True)

    if conn.login and conn.password:
        sess.auth = (conn.login, conn.password)

    host = conn.host
    scheme = extra.get("http_scheme", "https")
    port = conn.port or (8443 if scheme == "https" else 8080)

    sess.trino_endpoint = f"{scheme}://{host}:{port}/v1/statement"
    sess.headers.update(
        {
            "X-Trino-User": conn.login or "airflow",
            "X-Trino-Source": "salesorderdetail_ingestion",
        }
    )
    return sess


def trino_sql(sql: str, conn_id: str = TRINO_CONN_ID) -> dict:
    sess = get_trino_session(conn_id)

    r = sess.post(sess.trino_endpoint, data=sql.encode(), headers=sess.headers)
    r.raise_for_status()
    payload = r.json()

    all_data = []
    if "data" in payload:
        all_data.extend(payload["data"])

    next_uri = payload.get("nextUri")
    while next_uri:
        r = sess.get(next_uri, headers=sess.headers)
        r.raise_for_status()
        payload = r.json()

        if "error" in payload:
            raise RuntimeError(f"Trino error: {payload['error']}")

        if "data" in payload:
            all_data.extend(payload["data"])

        next_uri = payload.get("nextUri")

    if all_data:
        payload["data"] = all_data

    return payload


# =========================================
# HELPER: LẤY SCHEMA TỪ BẢNG ICEBERG
# =========================================
def get_iceberg_columns_ddl(trino_conn_id: str = TRINO_CONN_ID) -> str:
    sql = f"SHOW COLUMNS FROM {ICEBERG_TABLE}"
    result = trino_sql(sql, conn_id=trino_conn_id)
    data = result.get("data", [])
    if not data:
        raise RuntimeError(f"SHOW COLUMNS không trả về gì, payload = {result}")

    cols = [f"{row[0]} {row[1]}" for row in data]
    return ", ".join(cols)


# =========================================
# STEP 1 — CHECK MINIO CÓ FILE NGÀY run_date
# =========================================
def check_minio_for_run_date(run_date: str, **context):
    target_key = (
        f"{FAKE_PREFIX}/load_date={run_date}/salesorderdetail_{run_date}.parquet"
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    try:
        s3.head_object(Bucket=BUCKET, Key=target_key)
        print(f"[OK] Có dữ liệu trong MinIO: s3://{BUCKET}/{target_key}")
        return True
    except Exception:
        print(f"[WAIT] Chưa có dữ liệu ({run_date}). Chờ 5 phút nữa…")
        raise AirflowException("Chưa có dữ liệu trên MinIO")


# =========================================
# STEP 2 — CHECK + XÓA DỮ LIỆU NGÀY run_date TRONG ICEBERG (NẾU CÓ)
# =========================================
def check_and_cleanup_iceberg(run_date: str, trino_conn_id: str = TRINO_CONN_ID):
    sql_check = f"""
        SELECT COUNT(*) AS cnt
        FROM {ICEBERG_TABLE}
        WHERE etl_date = DATE '{run_date}'
    """
    result = trino_sql(sql_check, conn_id=trino_conn_id)
    data = result.get("data", [])
    if not data:
        raise RuntimeError(f"Không lấy được COUNT(*) từ Trino, payload = {result}")

    cnt = data[0][0]

    if cnt > 0:
        print(f"[INFO] Iceberg LIVE đã có {cnt} dòng etl_date = {run_date}. XÓA TRƯỚC.")
        sql_delete = f"""
            DELETE FROM {ICEBERG_TABLE}
            WHERE etl_date = DATE '{run_date}'
        """
        trino_sql(sql_delete, conn_id=trino_conn_id)
        print("[DONE] Xóa batch cũ xong.")
    else:
        print(f"[OK] Iceberg chưa có dữ liệu etl_date = {run_date}. Không cần xóa.")


# =========================================
# STEP 3 — INSERT PARQUET NGÀY run_date VÀO ICEBERG QUA BẢNG HIVE TẠM
# =========================================
def insert_into_iceberg(run_date: str, trino_conn_id: str = TRINO_CONN_ID):
    parquet_path = f"s3a://{BUCKET}/{FAKE_PREFIX}/load_date={run_date}/"
    temp_table = f"{HIVE_CATALOG}.{HIVE_SCHEMA}.tmp_fake_salesorderdetail_live"

    cols_ddl = get_iceberg_columns_ddl(trino_conn_id)

    sql_drop = f"DROP TABLE IF EXISTS {temp_table}"
    trino_sql(sql_drop, conn_id=trino_conn_id)

    sql_create = f"""
        CREATE TABLE {temp_table} ({cols_ddl})
        WITH (
            external_location = '{parquet_path}',
            format = 'PARQUET'
        )
    """
    print(f"[INFO] Tạo bảng tạm Hive: {temp_table}")
    trino_sql(sql_create, conn_id=trino_conn_id)

    # NOTE: Dù parquet có etl_date rồi, vẫn set lại etl_date = run_date cho chắc chắn theo batch
    sql_insert = f"""
        INSERT INTO {ICEBERG_TABLE} (
            salesorderid,
            salesorderdetailid,
            carriertrackingnumber,
            orderqty,
            productid,
            specialofferid,
            unitprice,
            unitpricediscount,
            linetotal,
            rowguid,
            modifieddate,
            etl_date
        )
        SELECT
            salesorderid,
            salesorderdetailid,
            carriertrackingnumber,
            orderqty,
            productid,
            specialofferid,
            unitprice,
            unitpricediscount,
            linetotal,
            rowguid,
            modifieddate,
            DATE '{run_date}' AS etl_date
        FROM {temp_table}
    """
    print(f"[INFO] Insert từ {temp_table} vào {ICEBERG_TABLE} cho ngày {run_date}…")
    trino_sql(sql_insert, conn_id=trino_conn_id)
    print(f"[DONE] Load ({run_date}) vào Iceberg thành công.")

    print(f"[INFO] Drop bảng tạm {temp_table}")
    trino_sql(sql_drop, conn_id=trino_conn_id)


# =========================================
# DAG
# =========================================
default_args = {
    "depends_on_past": False,
    "retries": 9999,
    "retry_delay": timedelta(seconds=CHECK_INTERVAL_SECS),
}

with DAG(
    dag_id="salesorderdetail_ingest_LIVE_full_pipeline_test",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["bronze", "fake", "iceberg", "ingestion"],
) as dag:

    run_date = "{{ dag_run.conf.get('run_date', macros.ds_add(ds, -1)) }}"

    wait_for_minio = PythonOperator(
        task_id="wait_for_minio_run_date",
        python_callable=check_minio_for_run_date,
        op_kwargs={"run_date": run_date},
    )

    cleanup_iceberg = PythonOperator(
        task_id="cleanup_partition_if_exists",
        python_callable=check_and_cleanup_iceberg,
        op_kwargs={"run_date": run_date},
    )

    load_to_iceberg = PythonOperator(
        task_id="insert_into_iceberg",
        python_callable=insert_into_iceberg,
        op_kwargs={"run_date": run_date},
    )

    trigger_dbt_silver = TriggerDagRunOperator(
        task_id="trigger_dbt_silver_salesorderdetail_LIVE",
        trigger_dag_id="dbt_run_model_selector",
        conf={
            # ✅ đổi đúng selector của mày
            "select": "LIVE_silver_sales_order_detail",
            "exclude": "",
            "full_refresh": False,
            "run_date": run_date,
        },
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
    )

    wait_for_minio >> cleanup_iceberg >> load_to_iceberg >> trigger_dbt_silver
