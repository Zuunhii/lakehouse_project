from __future__ import annotations

import random
from datetime import datetime, timedelta, date
from io import BytesIO

import boto3
import pandas as pd
from faker import Faker

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

# ========== CONFIG MINIO ==========
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "lakehouse"

TARGET_PREFIX = "bronze/adventureworks/Sales/Faker_SalesOrderHeader_daily"
fake = Faker()


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def generate_fake_sales_for_date(target_date: date, n_rows: int = 50) -> pd.DataFrame:
    rows = []
    valid_status = [1, 2, 3, 4, 5]
    territory_ids = list(range(1, 11))
    sales_people = list(range(280, 301))
    ship_methods = list(range(1, 6))

    # ====== TUNE ĐỘ BẨN (đổi các tỷ lệ này nếu muốn bẩn hơn/ít hơn) ======
    P_TOTALDUE_NULL = 0.15   # 5% totaldue = NULL -> raw DQ FAIL/WARN, clean vẫn fix
    P_TOTALDUE_NEG  = 0.25   # 15% totaldue âm -> raw DQ FAIL/WARN, clean vẫn fix
    P_TOTALDUE_WILD = 0.25   # 25% totaldue lệch rất mạnh (vẫn dương có thể) -> chuẩn bị cho rule mismatch sau này
    # =====================================================================

    for _ in range(n_rows):
        orderdate = datetime.combine(target_date, datetime.min.time()) + timedelta(days=random.randint(-2, 2))
        duedate = orderdate + timedelta(days=random.randint(3, 10))
        shipdate = orderdate + timedelta(days=random.randint(1, 7))

        subtotal = round(random.uniform(50, 500), 2)
        taxamt = round(subtotal * random.uniform(0.05, 0.15), 2)
        freight = round(random.uniform(5, 30), 2)

        # totaldue “bình thường”
        totaldue = round(subtotal + taxamt + freight, 2)

        fake_accountnumber = fake.bothify("10-40#####")
        fake_purchaseordernumber = fake.bothify("PO########")

        # ====== bẩn kiểu cũ (giữ lại) ======
        if random.random() > 0.9:
            subtotal = random.choice([
                round(random.uniform(-500, -50), 2),
                round(random.uniform(1000, 5000), 2)
            ])
        if random.random() > 0.9:
            taxamt = round(subtotal * random.uniform(0, 2), 2)
        if random.random() > 0.9:
            totaldue = subtotal - random.uniform(100, 200)
        # ================================

        # ====== bẩn kiểu mới: phá totaldue mạnh để raw DQ không PASS nhiều ======
        r = random.random()

        # 1) totaldue NULL (raw DQ bắt ngay), clean sẽ recompute nên vẫn OK
        if r < P_TOTALDUE_NULL:
            totaldue = None

        # 2) totaldue âm (raw DQ bắt ngay), clean sẽ recompute nên vẫn OK
        elif r < P_TOTALDUE_NULL + P_TOTALDUE_NEG:
            # âm “cho chắc”, biên đủ lớn để không bị rounding cứu
            totaldue = -abs(totaldue) - round(random.uniform(50, 500), 2)

        # 3) totaldue lệch cực mạnh (chuẩn bị cho rule mismatch nếu mày thêm sau này)
        elif r < P_TOTALDUE_NULL + P_TOTALDUE_NEG + P_TOTALDUE_WILD:
            totaldue = round(totaldue + random.uniform(-5000, 5000), 2)
        # ====================================================================

        rows.append(
            {
                "salesorderid": fake.random_int(min=600000, max=999999),
                "revisionnumber": 1,
                "orderdate": orderdate,
                "duedate": duedate,
                "shipdate": shipdate,
                "status": random.choice(valid_status),
                "onlineorderflag": True,
                "salesordernumber": f"SO{fake.random_int(100000, 999999)}",
                "purchaseordernumber": fake_purchaseordernumber,
                "accountnumber": fake_accountnumber,
                "customerid": random.randint(11000, 40000),
                "salespersonid": float(random.choice(sales_people)),
                "territoryid": random.choice(territory_ids),
                "billtoaddressid": random.randint(10000, 30000),
                "shiptoaddressid": random.randint(10000, 30000),
                "shipmethodid": random.choice(ship_methods),
                "creditcardid": float(random.randint(1000, 2000)),
                "creditcardapprovalcode": fake.bothify("APPROVED-######"),
                "currencyrateid": float(random.randint(1, 10)),
                "subtotal": subtotal,
                "taxamt": taxamt,
                "freight": freight,
                "totaldue": totaldue,
                "comment": "",
                "rowguid": fake.uuid4(),
                "modifieddate": datetime.combine(target_date, datetime.min.time()),
                "etl_date": target_date,
            }
        )

    return pd.DataFrame(rows)


def minio_file_exists(run_date_str: str) -> bool:
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
    key = f"{TARGET_PREFIX}/load_date={run_date}/salesorderheader_{run_date}.parquet"
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        print(f"[EXISTS] s3://{BUCKET}/{key}")
        return True
    except Exception:
        print(f"[NOT FOUND] s3://{BUCKET}/{key}")
        return False


def choose_fake_or_skip(run_date_str: str, **_):
    # Nếu đã có file -> đi thẳng trigger
    if minio_file_exists(run_date_str):
        return "skip_fake"
    # Chưa có -> fake trước
    return "generate_and_upload_fake_salesorderheader"


def upload_fake_daily_to_minio(run_date_str: str, n_rows: int = 50) -> None:
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()

    df_fake = generate_fake_sales_for_date(run_date, n_rows=n_rows)
    s3 = get_s3_client()

    key_prefix = f"{TARGET_PREFIX}/load_date={run_date}"
    key = f"{key_prefix}/salesorderheader_{run_date}.parquet"

    buffer = BytesIO()
    df_fake.to_parquet(buffer, index=False)
    buffer.seek(0)

    print(f"[UPLOAD FAKE] {run_date}: {len(df_fake)} dòng -> s3://{BUCKET}/{key}")
    s3.upload_fileobj(buffer, BUCKET, key)
    print("[DONE] Đã sinh & upload fake orders.")


default_args = {"depends_on_past": False, "retries": 0}

with DAG(
    dag_id="salesorderheader_daily_to_minio",
    start_date=pendulum.datetime(2025, 12, 1, tz=VN_TZ),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["faker", "minio", "salesorderheader"],
) as dag:

    run_date = "{{ dag_run.conf.get('run_date', data_interval_end.in_timezone('Asia/Ho_Chi_Minh').subtract(days=1).to_date_string()) }}"


    branch = BranchPythonOperator(
        task_id="check_exists_then_branch",
        python_callable=choose_fake_or_skip,
        op_kwargs={"run_date_str": run_date},
    )

    generate_and_upload = PythonOperator(
        task_id="generate_and_upload_fake_salesorderheader",
        python_callable=upload_fake_daily_to_minio,
        op_kwargs={"run_date_str": run_date, "n_rows": 100},
    )

    skip_fake = EmptyOperator(task_id="skip_fake")

    trigger_ingest_pipeline = TriggerDagRunOperator(
        task_id="trigger_salesorderheader_ingest_LIVE_full_pipeline",
        trigger_dag_id="salesorderheader_ingest_LIVE_full_pipeline",
        conf={"run_date": run_date},
        wait_for_completion=False,
        # ✅ Quan trọng: để trigger vẫn chạy dù 1 nhánh bị SKIPPED
        trigger_rule="none_failed_min_one_success",
    )

    branch >> [generate_and_upload, skip_fake]
    generate_and_upload >> trigger_ingest_pipeline
    skip_fake >> trigger_ingest_pipeline
