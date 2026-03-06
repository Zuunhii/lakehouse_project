from __future__ import annotations

import random
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import pandas as pd
from faker import Faker

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException
import pendulum

from botocore.exceptions import ClientError

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

# ========== CONFIG ==========
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "lakehouse"

HEADER_PREFIX = "bronze/adventureworks/Sales/Faker_SalesOrderHeader_daily"
DETAIL_PREFIX = "bronze/adventureworks/Sales/Faker_SalesOrderDetail_daily"

CHECK_INTERVAL_SECS = 5 * 60  # 5 phút
fake = Faker()

# ========== S3 ==========
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def s3_object_exists(bucket: str, key: str) -> bool:
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def read_parquet(bucket: str, key: str) -> pd.DataFrame:
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))

def upload_parquet(df: pd.DataFrame, bucket: str, key: str):
    s3 = get_s3_client()
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    print(f"[UPLOAD] {len(df)} rows -> s3://{bucket}/{key}")
    s3.upload_fileobj(buffer, bucket, key)

# ========== ID ==========
def make_salesorderdetailid(salesorderid: int, line_no: int) -> int:
    return salesorderid * 100 + line_no

# ========== BRANCH ==========
def choose_generate_or_skip(run_date_str: str, **_):
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
    detail_key = f"{DETAIL_PREFIX}/load_date={run_date}/salesorderdetail_{run_date}.parquet"

    if s3_object_exists(BUCKET, detail_key):
        print(f"[EXISTS] Detail đã có: s3://{BUCKET}/{detail_key} -> skip generate")
        return "skip_generate_detail"

    print(f"[NOT FOUND] Detail chưa có cho {run_date} -> generate")
    return "generate_and_upload_fake_salesorderdetail"

# ========== CORE ==========
def generate_and_upload_salesorderdetail(run_date_str: str, max_lines_per_order: int = 6):
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
    header_key = f"{HEADER_PREFIX}/load_date={run_date}/salesorderheader_{run_date}.parquet"

    # 🔁 Chờ header (retry nhờ AirflowException + retry_delay)
    try:
        df_header = read_parquet(BUCKET, header_key)
    except Exception:
        raise AirflowException(f"[WAIT] Header chưa tồn tại cho ngày {run_date}, retry sau 5 phút")

    salesorderids = (
        pd.to_numeric(df_header.get("salesorderid"), errors="coerce")
        .dropna()
        .astype("int64")
        .tolist()
    )

    if not salesorderids:
        raise AirflowException(f"[WAIT] Header {run_date} chưa có salesorderid, retry sau 5 phút")

    rows = []
    product_pool = list(range(700, 1000))
    offer_pool = list(range(1, 20))
    qty_pool = list(range(1, 11))
    price_min, price_max = 5.0, 500.0
    modified_ts = datetime.combine(run_date, datetime.min.time())

    # ====== thêm FAIL có chủ đích nhưng không đụng clean ======
    # clean drop khi: productid null OR orderqty null/<=0 OR unitprice null
    P_NULL_PRODUCT = 0.015   # 1.5% dòng -> FAIL (null key)
    P_NULL_PRICE   = 0.010   # 1.0% dòng -> FAIL/WARN (unitprice null)
    P_NULL_QTY     = 0.010   # 1.0% dòng -> FAIL/WARN (orderqty null)

    for soid in salesorderids:
        for line_no in range(1, random.randint(1, max_lines_per_order) + 1):
            orderqty = random.choice(qty_pool)
            unitprice = round(random.uniform(price_min, price_max), 2)
            unitpricediscount = round(random.uniform(0.0, 0.30), 4)

            # dirt nhẹ (giữ nguyên như bản đầu)
            if random.random() > 0.99:
                orderqty = 0
            if random.random() > 0.995:
                unitprice = -abs(unitprice)
            if random.random() > 0.99:
                unitpricediscount = random.choice([1.2, -0.1])

            # linetotal chuẩn + nhiễu mạnh hơn (chỉ phá linetotal)
            linetotal = round(unitprice * orderqty * (1 - unitpricediscount), 2)

            # Nhiễu thường xuyên hơn + biên lớn hơn
            if random.random() > 0.70:  # ~30% dòng bị nhiễu
                linetotal += round(random.uniform(-50, 50), 2)

            # Nhiễu "nặng" hiếm hơn để tạo outlier rõ ràng
            if random.random() > 0.97:  # ~3% dòng bị nhiễu rất mạnh
                linetotal += round(random.uniform(-200, 200), 2)

            # ====== inject FAIL nhưng không làm clean phải sửa ======
            productid = random.choice(product_pool)

            r = random.random()
            if r < P_NULL_PRODUCT:
                productid = None
            elif r < P_NULL_PRODUCT + P_NULL_PRICE:
                unitprice = None
            elif r < P_NULL_PRODUCT + P_NULL_PRICE + P_NULL_QTY:
                orderqty = None

            rows.append(
                {
                    "salesorderid": soid,
                    "salesorderdetailid": make_salesorderdetailid(soid, line_no),
                    "carriertrackingnumber": fake.bothify("?#?#########"),
                    "orderqty": orderqty,
                    "productid": productid,
                    "specialofferid": random.choice(offer_pool),
                    "unitprice": unitprice,
                    "unitpricediscount": unitpricediscount,
                    "linetotal": linetotal,
                    "rowguid": fake.uuid4(),
                    "modifieddate": modified_ts,
                    "etl_date": run_date,
                }
            )

    df = pd.DataFrame(rows)

    # ================= FIX CHÍNH Ở ĐÂY =================
    # Khi có None trong cột int, pandas sẽ biến cả cột thành float => parquet ghi double => Trino đọc lỗi.
    # Ép về nullable Int64 để parquet ghi INT64 (optional) đúng với schema BIGINT của Trino.
    int_cols = ["salesorderid", "salesorderdetailid", "orderqty", "productid", "specialofferid"]
    for c in int_cols:
        df[c] = pd.array(df[c], dtype="Int64")

    float_cols = ["unitprice", "unitpricediscount", "linetotal"]
    for c in float_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    df["modifieddate"] = pd.to_datetime(df["modifieddate"], errors="coerce")
    df["etl_date"] = pd.to_datetime(df["etl_date"], errors="coerce").dt.date
    # ===================================================

    detail_key = f"{DETAIL_PREFIX}/load_date={run_date}/salesorderdetail_{run_date}.parquet"
    upload_parquet(df, BUCKET, detail_key)
    print(f"[DONE] Fake detail (linetotal noisy + some FAIL rows) cho ngày {run_date}")

# ========== DAG ==========
default_args = {
    "depends_on_past": False,
    "retries": 9999,  # retry cho case chờ header
    "retry_delay": timedelta(seconds=CHECK_INTERVAL_SECS),
}

with DAG(
    dag_id="salesorderdetail_daily_to_minio_test",
    start_date=pendulum.datetime(2025, 12, 9, tz=VN_TZ),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["faker", "minio", "salesorderdetail"],
) as dag:

    run_date = "{{ dag_run.conf.get('run_date', data_interval_end.in_timezone('Asia/Ho_Chi_Minh').subtract(days=1).to_date_string()) }}"

    branch = BranchPythonOperator(
        task_id="check_detail_exists_then_branch",
        python_callable=choose_generate_or_skip,
        op_kwargs={"run_date_str": run_date},
    )

    gen_detail = PythonOperator(
        task_id="generate_and_upload_fake_salesorderdetail",
        python_callable=generate_and_upload_salesorderdetail,
        op_kwargs={"run_date_str": run_date, "max_lines_per_order": 6},
    )

    skip_generate = EmptyOperator(task_id="skip_generate_detail")

    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_salesorderdetail_ingest_LIVE_full_pipeline",
        trigger_dag_id="salesorderdetail_ingest_LIVE_full_pipeline_test",
        conf={"run_date": run_date},
        wait_for_completion=False,
        trigger_rule="none_failed_min_one_success",
    )

    branch >> [gen_detail, skip_generate]
    gen_detail >> trigger_pipeline
    skip_generate >> trigger_pipeline
