from datetime import datetime, timedelta
from airflow import DAG
from cosmos.operators import DbtRunOperator
from cosmos import ProfileConfig

with DAG(
    dag_id="dbt_run_model_selector",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,  # đủ để tránh chồng run cho DAG này
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=10)},
    tags=["dbt", "adhoc", "cosmos"],
    params={
        "select": "dim_address",
        "exclude": "",
        "full_refresh": False,
    },
) as dag:

    DbtRunOperator(
        task_id="dbt_run_selected",
        project_dir="/opt/airflow/dbt",
        profile_config=ProfileConfig(
            profile_name="dbt_trino_project",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/dbt/profiles.yml",
        ),
        dbt_bin="/home/airflow/.local/bin/dbt",

        select="{{ params.select }}",
        exclude="{{ params.exclude }}",
        full_refresh="{{ params.full_refresh }}",

    )



