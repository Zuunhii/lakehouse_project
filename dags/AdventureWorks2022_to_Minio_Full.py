from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
import boto3
from io import BytesIO
import uuid
import base64
import pyarrow as pa
import pyarrow.parquet as pq
import decimal

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "lakehouse"
CATALOG = "AdventureWorks2022"

def _coerce_chunk(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        s = df[col]

        # Nếu toàn NaN, vẫn ép string để cố định schema UTF8
        if s.notna().sum() == 0:
            df[col] = s.astype("string")
            continue

        # UUID -> string
        try:
            if s.map(lambda x: isinstance(x, uuid.UUID)).any():
                df[col] = s.astype(str)
                continue
        except Exception:
            pass

        # Bytes -> base64 string
        try:
            has_bytes = s.map(lambda x: isinstance(x, (bytes, bytearray))).any()
        except Exception:
            has_bytes = False
        if has_bytes:
            df[col] = s.map(
                lambda x: base64.b64encode(x).decode("ascii")
                if isinstance(x, (bytes, bytearray)) else (None if pd.isna(x) else str(x))
            )
            continue

        # Decimal -> string (hoặc chuyển Decimal->string nhất quán nếu bạn muốn bảo toàn scale)
        try:
            has_decimal = s.map(lambda x: isinstance(x, decimal.Decimal)).any()
        except Exception:
            has_decimal = False
        if has_decimal:
            df[col] = s.map(lambda x: str(x) if isinstance(x, decimal.Decimal) else (None if pd.isna(x) else x))
            continue

        # Object khác -> StringDtype (đảm bảo NVARCHAR -> UTF8)
        if pd.api.types.is_object_dtype(s):
            df[col] = s.astype("string")
            continue

    return df


def extract_full_db(**_):
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    engine = hook.get_sqlalchemy_engine()

    tables = pd.read_sql(
        f"""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM {CATALOG}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
          AND TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA')
        """,
        engine,
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    for _, r in tables.iterrows():
        schema, table = r["TABLE_SCHEMA"], r["TABLE_NAME"]
        print(f"Processing {CATALOG}.{schema}.{table}")

        query = f'SELECT * FROM [{CATALOG}].[{schema}].[{table}]'
        it = pd.read_sql(query, engine, chunksize=100_000)

        buf = BytesIO()
        writer = None
        row_count = 0

        for chunk in it:
            chunk = _coerce_chunk(chunk)
            row_count += len(chunk)

            tbl_pa = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(buf, tbl_pa.schema, compression="snappy")
            writer.write_table(tbl_pa)

        if writer is None:
            empty_tbl = pa.Table.from_pandas(pd.DataFrame(), preserve_index=False)
            writer = pq.ParquetWriter(buf, empty_tbl.schema, compression="snappy")
            writer.close()
        else:
            writer.close()

        buf.seek(0)

        # mỗi bảng một thư mục; file = <table>.parquet
        key = f"bronze/adventureworks/{schema}/{table}/{table}.parquet"

        s3.upload_fileobj(buf, BUCKET_NAME, key)
        print(f" Uploaded s3://{BUCKET_NAME}/{key} (rows={row_count})")

with DAG(
    dag_id="mssql_to_minio_full_db",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "adventureworks"],
) as dag:
    PythonOperator(
        task_id="extract_full_db",
        python_callable=extract_full_db,
    )
