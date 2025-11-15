# dags/dbt_daily_all.py
from datetime import datetime, timedelta
from airflow import DAG
from cosmos.operators import DbtRunOperator
from cosmos import ProfileConfig

with DAG(
    dag_id="dbt_daily_all",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,       # chạy hàng ngày
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(minutes=30),  # tránh treo
    },
    tags=["dbt", "daily", "cosmos"],
    params={
        # đổi sang True nếu muốn full refresh toàn bộ
        "full_refresh": False,
        # có thể filter theo tag hoặc schema
        "select": "*",                # chạy toàn bộ models
    },
) as dag:

    dbt_daily_run = DbtRunOperator(
        task_id="dbt_run_all_models",
        project_dir="/opt/airflow/dbt",
        profile_config=ProfileConfig(
            profile_name="dbt_trino_project",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/dbt/profiles.yml",
        ),
        dbt_bin="/home/airflow/.local/bin/dbt",

        # chạy tất cả (hoặc tag cụ thể)
        select="{{ params.select }}",
        full_refresh="{{ params.full_refresh }}",

        # threads set trong profiles.yml cho gọn
    )


