from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"
DBT_TARGET = "dev"

with DAG(
    dag_id="dbt_run_test_selector",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "gold"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            'set -xeuo pipefail; '
            'export PATH="$PATH:/home/airflow/.local/bin"; '
            f'cd {DBT_DIR}; '

            # deps + debug để check kết nối
            'dbt deps -v; '
            'dbt debug --profiles-dir . --target ' + DBT_TARGET + ' -v; '

            # Selector/Exclude/Full refresh
            'SELECTOR="{{ dag_run.conf.get(\'select\', \'dim_address\') }}"; '
            'EXCLUDE="{{ dag_run.conf.get(\'exclude\', \'\') }}"; '
            'REFRESH="{{ dag_run.conf.get(\'full_refresh\', \'false\') }}"; '
            'FR_FLAG=""; if [ "$REFRESH" = "true" ] || [ "$REFRESH" = "1" ]; then FR_FLAG="--full-refresh"; fi; '

            # Liệt kê model match (names only)
            'echo "== dbt ls (names only) =="; '
            'MATCHED=$(dbt ls --profiles-dir . --target ' + DBT_TARGET + ' '
                '--quiet --resource-type model --output name '
                '--select "$SELECTOR" ${EXCLUDE:+--exclude "$EXCLUDE"} 2>/dev/null || true); '
            'echo "$MATCHED"; '
            'COUNT=$(printf "%s\n" "$MATCHED" | sed "/^[[:space:]]*$/d" | wc -l); '
            'echo "Matched models: $COUNT"; '
            'test "$COUNT" -gt 0; '

            # In compiled SQL cho từng model match (nếu có)
            'echo "== compiled SQL preview =="; '
            'shopt -s nullglob; '
            'for f in target/run/*/models/**.sql; do '
            '  echo "---- $f ----"; sed -n "1,200p" "$f" || true; '
            'done || true; '

            # Chạy run ở mức debug + no-color để log dễ đọc trong Airflow
            'echo "== dbt run =="; '
            'dbt run --profiles-dir . --target ' + DBT_TARGET + ' '
                '--select "$SELECTOR" ${EXCLUDE:+--exclude "$EXCLUDE"} $FR_FLAG '
                '--debug --no-use-colors --fail-fast; '

            # Đọc run_results.json và ép fail nếu không success
            'echo "== Inspect run_results.json =="; '
            'python - <<\'PY\'\n'
            'import json, sys, pathlib\n'
            'p = pathlib.Path("target/run_results.json")\n'
            'if not p.exists():\n'
            '    print("run_results.json not found -> FAIL", file=sys.stderr); sys.exit(1)\n'
            'd = json.loads(p.read_text())\n'
            'nodes = d.get("results", [])\n'
            'total = len(nodes)\n'
            'failed = [n for n in nodes if n.get("status") != "success"]\n'
            'print(f"Total executed nodes: {total}")\n'
            'for n in nodes:\n'
            '    print(f" - {n.get(\'unique_id\')} -> {n.get(\'status\')} ({n.get(\'execution_time\')}s)")\n'
            'if total == 0 or failed:\n'
            '    print("dbt run did not execute any nodes or some failed.", file=sys.stderr)\n'
            '    sys.exit(1)\n'
            'PY\n'
        ),
        env={
            "DBT_PROFILES_DIR": DBT_DIR,
            # log dbt rõ hơn, ghi file logs/dbt.log
            "DBT_LOG_LEVEL": "debug",
            "DBT_LOG_FORMAT": "text"
        },
        retries=0,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            'set -xeuo pipefail; '
            'export PATH="$PATH:/home/airflow/.local/bin"; '
            f'cd {DBT_DIR}; '

            # Lấy selector/exclude từ dag_run.conf (mặc định dim_address)
            'SELECTOR="{{ dag_run.conf.get(\'select\', \'dim_address\') }}"; '
            'EXCLUDE="{{ dag_run.conf.get(\'exclude\', \'\') }}"; '

            'echo "== dbt test =="; '
            'dbt test --profiles-dir . --target ' + DBT_TARGET + ' '
                '--select "$SELECTOR" ${EXCLUDE:+--exclude "$EXCLUDE"} '
                '--debug --no-use-colors --fail-fast; '

            'echo "== Inspect test run_results.json =="; '
            'python - <<\'PY\'\n'
            'import json, sys, pathlib\n'
            'p = pathlib.Path("target/run_results.json")\n'
            'if not p.exists():\n'
            '    print("run_results.json not found", file=sys.stderr)\n'
            '    sys.exit(1)\n'
            'd = json.loads(p.read_text())\n'
            'nodes = d.get("results", [])\n'
            'print(f"Total test nodes: {len(nodes)}")\n'
            'bad = []\n'
            'ALLOWED = {"pass","success","skipped","warn"}  # coi warn/skip là không fail\n'
            'for n in nodes:\n'
            '    uid = n.get("unique_id")\n'
            '    status = (n.get("status") or "").lower()\n'
            '    et = n.get("execution_time")\n'
            '    print(f" - {uid} -> {status} ({et}s)")\n'
            '    if status not in ALLOWED:\n'
            '        bad.append(uid)\n'
            'if bad:\n'
            '    print("dbt test failing: " + ", ".join(bad), file=sys.stderr)\n'
            '    sys.exit(1)\n'
            'PY\n'
        ),
        env={
            "DBT_PROFILES_DIR": DBT_DIR,
            "DBT_LOG_LEVEL": "debug",
            "DBT_LOG_FORMAT": "text"
        },
        retries=0,
    )


    dbt_run >> dbt_test
