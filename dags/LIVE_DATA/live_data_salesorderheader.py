from __future__ import annotations
import random
from datetime import datetime, timedelta, date
from io import BytesIO
import boto3
import pandas as pd
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



# ========== CONFIG MINIO ==========
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "lakehouse"

# Nơi để data daily
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
    """
    Sinh n_rows đơn hàng fake cho 1 ngày (orderdate xoay quanh target_date).
    """
    rows = []
    
    # Chỉnh lại phạm vi dữ liệu sao cho sát với AdventureWorks
    valid_status = [1, 2, 3, 4, 5]  # Giả sử các trạng thái: 1 = New, 2 = In Progress, 3 = Shipped, 4 = Closed, 5 = Canceled
    territory_ids = list(range(1, 11))  # Giả sử có 10 vùng lãnh thổ
    sales_people = list(range(280, 301))  # Giả sử có 20 nhân viên bán hàng
    ship_methods = list(range(1, 6))  # 5 phương thức vận chuyển

    for _ in range(n_rows):
        orderdate = datetime.combine(target_date, datetime.min.time()) + timedelta(days=random.randint(-2, 2))
        duedate = orderdate + timedelta(days=random.randint(3, 10))
        shipdate = orderdate + timedelta(days=random.randint(1, 7))

        subtotal = round(random.uniform(50, 500), 2)
        taxamt = round(subtotal * random.uniform(0.05, 0.15), 2)
        freight = round(random.uniform(5, 30), 2)
        totaldue = round(subtotal + taxamt + freight, 2) + random.uniform(-10, 10)

        # Dữ liệu bẩn (giả lập thông tin không hợp lệ)
        fake_accountnumber = fake.bothify("10-40#####")  # Dữ liệu không chuẩn
        fake_purchaseordernumber = fake.bothify("PO########")  # Dữ liệu không chuẩn

        # Làm bẩn subtotal (giá trị âm hoặc quá lớn)
        if random.random() > 0.9:  # 10% xác suất tạo subtotal không hợp lý
            subtotal = random.choice([round(random.uniform(-500, -50), 2), round(random.uniform(1000, 5000), 2)])
        
        # Làm bẩn thuế (quá cao hoặc quá thấp)
        if random.random() > 0.9:
            taxamt = round(subtotal * random.uniform(0, 2), 2)

        # Làm bẩn tổng tiền cần thanh toán (không hợp lý)
        if random.random() > 0.9:
            totaldue = subtotal - random.uniform(100, 200)  # Tạo tổng tiền âm

        row = {
            "salesorderid": fake.random_int(min=600000, max=999999),  # ID ngẫu nhiên từ 600000 đến 999999
            "revisionnumber": 1,
            "orderdate": orderdate,
            "duedate": duedate,
            "shipdate": shipdate,
            "status": random.choice(valid_status),  # Status giả định theo AdventureWorks
            "onlineorderflag": True,
            "salesordernumber": f"SO{fake.random_int(100000, 999999)}",  # Mã đơn hàng theo mẫu SO######
            "purchaseordernumber": fake_purchaseordernumber,  # Dữ liệu bẩn
            "accountnumber": fake_accountnumber,  # Dữ liệu bẩn
            "customerid": random.randint(11000, 40000),  # Phạm vi ID khách hàng
            "salespersonid": float(random.choice(sales_people)),  # Chọn ngẫu nhiên nhân viên bán hàng
            "territoryid": random.choice(territory_ids),  # Chọn ngẫu nhiên vùng lãnh thổ
            "billtoaddressid": random.randint(10000, 30000),  # Chọn ngẫu nhiên địa chỉ
            "shiptoaddressid": random.randint(10000, 30000),  # Chọn ngẫu nhiên địa chỉ
            "shipmethodid": random.choice(ship_methods),  # Phương thức vận chuyển ngẫu nhiên
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

        rows.append(row)

    df = pd.DataFrame(rows)
    return df

def upload_fake_daily_to_minio(run_date_str: str, n_rows: int = 50) -> None:
    """
    Sinh n_rows đơn hàng cho ngày run_date_str (YYYY-MM-DD) và upload vào MinIO.
    """
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()

    df_fake = generate_fake_sales_for_date(run_date, n_rows=n_rows)
    s3 = get_s3_client()

    # Path trên MinIO: .../load_date=YYYY-MM-DD/salesorderheader_YYYY-MM-DD.parquet
    key_prefix = f"{TARGET_PREFIX}/load_date={run_date}"
    key = f"{key_prefix}/salesorderheader_{run_date}.parquet"

    buffer = BytesIO()
    df_fake.to_parquet(buffer, index=False)
    buffer.seek(0)

    print(f"[UPLOAD FAKE] {run_date}: {len(df_fake)} dòng -> s3://{BUCKET}/{key}")
    s3.upload_fileobj(buffer, BUCKET, key)
    print("[DONE] Đã sinh & upload fake orders.")

# ========== DAG DEFINITION ==========
default_args = {
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="salesorderheader_daily_to_minio",
    start_date=days_ago(1),
    schedule_interval="@daily",     # chạy hằng ngày
    catchup=False,           # không backfill
    default_args=default_args,
    tags=["faker", "minio", "salesorderheader"],
) as dag:

    # Đặt ngày T-1 (ngày hôm qua)
    run_date = "{{ dag_run.conf.get('run_date', macros.ds_add(ds, -1)) }}"  # Nếu không có run_date từ UI, lấy T-1

    generate_and_upload_fake_salesorderheader = PythonOperator(
        task_id="generate_and_upload_fake_salesorderheader",
        python_callable=upload_fake_daily_to_minio,
        op_kwargs={
            "run_date_str": run_date,  # Airflow sẽ truyền ngày T-1
            "n_rows": 100,              # Chỉnh số dòng fake/ ngày ở đây
        },
    )

    # 🟢 TASK NÀY LÀ CHỖ GỌI SANG DAG FULL PIPELINE
    trigger_ingest_pipeline = TriggerDagRunOperator(
        task_id="trigger_salesorderheader_ingest_LIVE_full_pipeline",
        trigger_dag_id="salesorderheader_ingest_LIVE_full_pipeline",  # tên DAG full
        conf={
            # Truyền đúng ngày mà thằng full pipeline sẽ xử lý
            # (ở DAG full đang dùng dag_run.conf['run_date'])
            "run_date": run_date,
        },
        wait_for_completion=False,  # không cần ngồi chờ DAG kia xong
    )

    # Flow: fake xong -> trigger DAG full
    generate_and_upload_fake_salesorderheader >> trigger_ingest_pipeline