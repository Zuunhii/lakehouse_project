from datetime import datetime, timedelta
from airflow import DAG
from cosmos.operators import DbtBuildOperator, DbtRunOperator
from cosmos import ProfileConfig

with DAG(
    dag_id="dbt_run_model_selector",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=10)},
    tags=["dbt", "adhoc", "cosmos"],
    params={
        "select": "dim_address",
        "exclude": "",
        "full_refresh": False,
        "run_date": None,  # Default to None, will be replaced if passed in dag_run.conf
    },
) as dag:

    DbtBuildOperator(
        task_id="dbt_run_selected",
        project_dir="/opt/airflow/dbt",
        profile_config=ProfileConfig(
            profile_name="dbt_trino_project",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/dbt/profiles.yml",
        ),
        dbt_bin="/home/airflow/.local/bin/dbt",

        select="{{ dag_run.conf.get('select', params.select) }}",
        exclude="{{ dag_run.conf.get('exclude', params.exclude) }}",
        
        # Use the correct template syntax for full_refresh
        full_refresh="{{ dag_run.conf.get('full_refresh', params.full_refresh) }}",  # Boolean value

        # Pass the run_date directly to DBT
        vars={
            "run_date": "{{ dag_run.conf.get('run_date', params.run_date) }}"  # This will use the value from dag_run.conf or fallback to params
        },
    )
