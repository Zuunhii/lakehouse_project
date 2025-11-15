from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ===== DANH SÁCH BẢNG SILVER THEO THỨ TỰ MUỐN CHẠY (TUẦN TỰ) =====
TABLES = [
    # PERSON
    {"schema": "Person", "table": "Address"},
    {"schema": "Person", "table": "CountryRegion"},
    {"schema": "Person", "table": "Person"},
    {"schema": "Person", "table": "StateProvince"},

    # PRODUCTION
    {"schema": "Production", "table": "Product"},
    {"schema": "Production", "table": "ProductSubcategory"},
    {"schema": "Production", "table": "ProductCategory"},

    # SALES
    {"schema": "Sales", "table": "CreditCard"},
    {"schema": "Sales", "table": "Customer"},
    {"schema": "Sales", "table": "SalesOrderDetail"},
    {"schema": "Sales", "table": "SalesOrderHeader"},
    {"schema": "Sales", "table": "SalesOrderHeaderSalesReason"},
    {"schema": "Sales", "table": "SalesReason"},
    {"schema": "Sales", "table": "Store"},

]

with DAG(
    dag_id="e2e_silver_then_gold_sequential_manual",
    start_date=datetime(2025, 1, 1),
    schedule=None,                     # 👈 chỉ chạy khi bạn trigger thủ công
    catchup=False,
    max_active_runs=1,
    concurrency=1,                     # đảm bảo tuần tự trong DAG
    tags=["e2e", "manual", "silver", "gold", "dbt"],
    default_args={"retries": 0, "retry_delay": timedelta(minutes=3)},
) as dag:

    start = EmptyOperator(task_id="start")

    # === ĐĂNG KÝ SILVER TUẦN TỰ (BẢNG NÀY XONG MỚI SANG BẢNG KIA) ===
    prev = start
    for t in TABLES:
        schema = t["schema"]
        table = t["table"]
        task_id = f"register__{schema.lower()}__{table.lower()}"
        reg = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="register_iceberg_single_table",
            wait_for_completion=True,      # đợi DAG con xong rồi mới chạy tiếp
            poke_interval=10,
            reset_dag_run=True,            # bỏ nếu Airflow < 2.7
            conf={"schema": schema, "table": table},
        )
        prev >> reg
        prev = reg

    # === SAU KHI TOÀN BỘ REGISTER THÀNH CÔNG, CHẠY DBT (GOLD) ===
    run_dbt_gold = TriggerDagRunOperator(
        task_id="run_dbt_gold",
        trigger_dag_id="dbt_run_test_selector",
        wait_for_completion=True,
        poke_interval=15,
        reset_dag_run=True,
        conf={"select": "tag:gold", "exclude": "", "full_refresh": "false"},
        # all_success: nếu 1 silver fail -> gold không chạy
    )

    done = EmptyOperator(task_id="done")

    prev >> run_dbt_gold >> done
