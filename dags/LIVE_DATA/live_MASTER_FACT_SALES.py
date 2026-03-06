from __future__ import annotations

import pendulum
import urllib3
from datetime import timedelta
from requests import Session

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
urllib3.disable_warnings()

TRINO_CONN_ID = "trino_default"

SILVER_HEADER_TABLE = "iceberg.silver.silver_sales_salesorderheader_live"
SILVER_DETAIL_TABLE = "iceberg.silver.silver_sales_salesorderdetail_live"

# ✅ dùng lowercase để khỏi dính case-folding
FACT_TABLE = "iceberg.marts.fact_sales_live"

DAG_FAKE_HEADER = "salesorderheader_daily_to_minio"
DAG_FAKE_DETAIL = "salesorderdetail_daily_to_minio_test"

DBT_SELECTOR_DAG = "dbt_run_model_selector"
DBT_SELECT = "fact_sales_LIVE"  # cái này là dbt node name/selector của mày, giữ nguyên

POKE_SECS = 60
CHECK_INTERVAL_SECS = 60  # retry mỗi phút
MAX_RETRIES = 180         # ~3 tiếng (180 phút) tuỳ mày chỉnh


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
        {"X-Trino-User": conn.login or "airflow", "X-Trino-Source": "sales_master_to_fact"}
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


def assert_silver_ready(run_date: str):
    sql = f"""
    SELECT
      (SELECT COUNT(*) FROM {SILVER_HEADER_TABLE} WHERE etl_date = DATE '{run_date}') AS cnt_h,
      (SELECT COUNT(*) FROM {SILVER_DETAIL_TABLE} WHERE etl_date = DATE '{run_date}') AS cnt_d
    """
    res = trino_sql(sql)
    cnt_h, cnt_d = res["data"][0][0], res["data"][0][1]
    print(f"[CHECK] run_date={run_date} header={cnt_h} detail={cnt_d}")

    # ✅ chưa đủ thì raise để Airflow retry
    if cnt_h == 0 or cnt_d == 0:
        raise AirflowException(f"Silver not ready for {run_date}: header={cnt_h}, detail={cnt_d}")


def cleanup_fact_partition_if_exists(run_date: str):
    sql = f"DELETE FROM {FACT_TABLE} WHERE etl_date = DATE '{run_date}'"
    print(f"[CLEANUP] fact etl_date={run_date} table={FACT_TABLE}")

    try:
        trino_sql(sql)
        print("[DONE] cleanup fact")
    except RuntimeError as e:
        msg = str(e).lower()

        # ✅ bảng chưa có thì skip (lần đầu chạy)
        if "table_not_found" in msg or "does not exist" in msg:
            print(f"[SKIP] Fact table not found yet: {FACT_TABLE}. Skip cleanup.")
            return

        # còn lỗi khác thì fail thật
        raise


default_args = {
    "depends_on_past": False,
}

with DAG(
    dag_id="sales_master_to_fact_sales_LIVE",
    start_date=pendulum.datetime(2025, 12, 1, tz=VN_TZ),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["master", "sales", "fact"],
) as dag:

    run_date = "{{ dag_run.conf.get('run_date', data_interval_end.in_timezone('Asia/Ho_Chi_Minh').subtract(days=1).to_date_string()) }}"

    trigger_fake_header = TriggerDagRunOperator(
        task_id="trigger_fake_header_and_wait",
        trigger_dag_id=DAG_FAKE_HEADER,
        conf={"run_date": run_date},
        wait_for_completion=True,
        poke_interval=POKE_SECS,
        reset_dag_run=True,
    )

    trigger_fake_detail = TriggerDagRunOperator(
        task_id="trigger_fake_detail_and_wait",
        trigger_dag_id=DAG_FAKE_DETAIL,
        conf={"run_date": run_date},
        wait_for_completion=True,
        poke_interval=POKE_SECS,
        reset_dag_run=True,
    )

    check_silver = PythonOperator(
        task_id="wait_until_silver_ready",
        python_callable=assert_silver_ready,
        op_kwargs={"run_date": run_date},
        retries=MAX_RETRIES,
        retry_delay=timedelta(seconds=CHECK_INTERVAL_SECS),
    )

    cleanup_fact = PythonOperator(
        task_id="cleanup_fact_partition_if_exists",
        python_callable=cleanup_fact_partition_if_exists,
        op_kwargs={"run_date": run_date},
        retries=MAX_RETRIES,
        retry_delay=timedelta(seconds=CHECK_INTERVAL_SECS),
    )

    trigger_dbt_fact = TriggerDagRunOperator(
        task_id="trigger_dbt_fact_sales_LIVE",
        trigger_dag_id=DBT_SELECTOR_DAG,
        conf={
            "select": DBT_SELECT,
            "exclude": "",
            "full_refresh": False,
            "run_date": run_date,
        },
        wait_for_completion=True,
        poke_interval=POKE_SECS,
        reset_dag_run=True,
    )
    
    trigger_dq_dag = TriggerDagRunOperator(
    task_id="trigger_dq_dag",
    trigger_dag_id="sales_dq_run_4tables",
    conf={"run_date": run_date},
    wait_for_completion=False,   # ✅ master không chờ
    reset_dag_run=True,
    )

    trigger_fake_header >> trigger_fake_detail >> check_silver >> cleanup_fact >> trigger_dbt_fact>>trigger_dq_dag
