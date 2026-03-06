from __future__ import annotations

import os
import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from trino.dbapi import connect
from trino.auth import BasicAuthentication

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

DBT_SELECTOR_DAG = "dbt_run_model_selector"
DQ_LLM_DAG = "dq_llm_classifier"

DBT_SELECT_DQ = " ".join([
    "dq_sales_order_header_raw",
    "dq_sales_order_header_clean",
    "dq_sales_order_detail_raw",
    "dq_sales_order_detail_clean",
    "dq_compare_salesorderdetail_live",
    "dq_compare_salesorderheader_live",
    "dq_compare_daily_summary",
])

POKE_SECS = 60
default_args = {"depends_on_past": False}

TRINO_CONN_ID = "trino_default"
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg").strip()
SCHEMA_SILVER = os.getenv("TRINO_SCHEMA_SILVER", "silver").strip()
SCHEMA_GOLD = os.getenv("TRINO_SCHEMA_GOLD", "gold").strip()


def _get_run_date_from_context(context) -> str:
    conf = (context.get("dag_run").conf or {})
    if conf.get("run_date"):
        return conf["run_date"]
    # fallback giống mày đang dùng
    return context["data_interval_end"].in_timezone("Asia/Ho_Chi_Minh").subtract(days=1).to_date_string()


def _trino_conn(schema: str):
    c = BaseHook.get_connection(TRINO_CONN_ID)
    extra = c.extra_dejson or {}

    catalog = extra.get("catalog") or TRINO_CATALOG
    http_scheme = extra.get("http_scheme") or "https"

    verify = extra.get("verify", False)
    if isinstance(verify, str):
        verify = verify.strip().lower() in ("1", "true", "yes", "y")

    auth = None
    if c.password:
        auth = BasicAuthentication(c.login, c.password)

    return connect(
        host=c.host,
        port=int(c.port or 8443),
        user=c.login,
        auth=auth,
        http_scheme=http_scheme,
        verify=verify,
        catalog=catalog,
        schema=schema,
    )


def _exec_sql(conn, sql: str) -> None:
    cur = conn.cursor()
    cur.execute(sql)
    # Trino thường auto-commit; vẫn ok


def _exists_any(conn, table_fqn: str, run_date: str) -> bool:
    sql = f"SELECT 1 FROM {table_fqn} WHERE etl_date = DATE '{run_date}' LIMIT 1"
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchone() is not None


def prepare_cleanup(**context):
    run_date = _get_run_date_from_context(context)
    print(f"[prepare_cleanup] run_date={run_date}")

    # 7 bảng output cần dọn trước khi rerun (nếu đã có data)
    tables = [
        f"{TRINO_CATALOG}.{SCHEMA_SILVER}.dq_sales_order_detail_raw",
        f"{TRINO_CATALOG}.{SCHEMA_SILVER}.dq_sales_order_detail_clean",
        f"{TRINO_CATALOG}.{SCHEMA_SILVER}.dq_sales_order_header_raw",
        f"{TRINO_CATALOG}.{SCHEMA_SILVER}.dq_sales_order_header_clean",
        f"{TRINO_CATALOG}.{SCHEMA_SILVER}.dq_compare_salesorderheader_live",
        f"{TRINO_CATALOG}.{SCHEMA_SILVER}.dq_compare_salesorderdetail_live",
        f"{TRINO_CATALOG}.{SCHEMA_GOLD}.dq_compare_daily_summary",
        f"{TRINO_CATALOG}.{SCHEMA_GOLD}.dq_agent_report_daily",
    ]

    # Dùng connection schema gold (schema nào cũng query được vì đã fully-qualified)
    conn = _trino_conn(SCHEMA_GOLD)

    # Check nhanh: chỉ cần 1 bảng có data là coi như rerun -> dọn hết cho sạch
    has_data = False
    for t in tables:
        try:
            if _exists_any(conn, t, run_date):
                has_data = True
                print(f"[prepare_cleanup] found existing data in: {t}")
                break
        except Exception as e:
            raise AirflowException(f"[prepare_cleanup] exists-check failed for {t}: {e}")

    if not has_data:
        print("[prepare_cleanup] no existing data for this run_date -> skip delete")
        return

    # Nếu đã có data -> DELETE hết cho ngày đó
    for t in tables:
        try:
            del_sql = f"DELETE FROM {t} WHERE etl_date = DATE '{run_date}'"
            print(f"[prepare_cleanup] deleting: {t}")
            _exec_sql(conn, del_sql)
        except Exception as e:
            raise AirflowException(f"[prepare_cleanup] delete failed for {t}: {e}")

    print("[prepare_cleanup] cleanup done")


with DAG(
    dag_id="sales_dq_run_4tables",
    start_date=pendulum.datetime(2025, 12, 1, tz=VN_TZ),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["dq", "sales"],
) as dag:

    run_date = "{{ dag_run.conf.get('run_date', data_interval_end.in_timezone('Asia/Ho_Chi_Minh').subtract(days=1).to_date_string()) }}"

    t_prepare_cleanup = PythonOperator(
        task_id="prepare_cleanup_run_date",
        python_callable=prepare_cleanup,
    )

    trigger_dbt_dq = TriggerDagRunOperator(
        task_id="trigger_dbt_selector_run_dq_models",
        trigger_dag_id=DBT_SELECTOR_DAG,
        conf={
            "select": DBT_SELECT_DQ,
            "exclude": "",
            "full_refresh": False,
            "run_date": run_date,
        },
        wait_for_completion=True,
        poke_interval=POKE_SECS,
        reset_dag_run=True,
    )

    trigger_llm_report = TriggerDagRunOperator(
        task_id="trigger_dq_llm_classifier",
        trigger_dag_id=DQ_LLM_DAG,
        conf={"run_date": run_date},
        wait_for_completion=True,
        poke_interval=POKE_SECS,
        reset_dag_run=True,
    )

    t_prepare_cleanup >> trigger_dbt_dq >> trigger_llm_report
