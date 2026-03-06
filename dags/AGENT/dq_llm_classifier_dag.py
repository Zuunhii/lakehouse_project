from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess
from airflow.exceptions import AirflowException
    
SCRIPT_PATH = "/opt/airflow/dags/AGENT/dq_llm_classifier.py"

def run_llm_classifier(**context):
    run_date = (context.get("dag_run").conf or {}).get("run_date") or context["ds"]

    p = subprocess.run(
        ["python", SCRIPT_PATH, "--run_date", run_date],
        capture_output=True,
        text=True,
    )

    # Đẩy output ra log Airflow
    if p.stdout:
        print("=== dq_llm_classifier STDOUT ===")
        print(p.stdout)
    if p.stderr:
        print("=== dq_llm_classifier STDERR ===")
        print(p.stderr)

    if p.returncode != 0:
        raise AirflowException(f"dq_llm_classifier failed with return code {p.returncode}")

with DAG(
    dag_id="dq_llm_classifier",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["dq", "llm"],
) as dag:

    t1 = PythonOperator(
        task_id="run_llm_classifier",
        python_callable=run_llm_classifier,
    )
