from airflow.models import Variable
import pandas as pd
import boto3
from io import BytesIO
import uuid
import base64
import decimal
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine

# ==============================
# CONFIG
# ==============================

# MSSQL CONNECTION (AdventureWorksDW chạy trên Windows)
MSSQL_HOST = "host.docker.internal"  # ví dụ: 192.168.1.10
MSSQL_USER = "sa"
MSSQL_PASSWORD = "Zuunhii03@"
MSSQL_DB = "AdventureWorksDW2022"

mssql_url = f"mssql+pyodbc://{MSSQL_USER}:{MSSQL_PASSWORD}@{MSSQL_HOST}/{MSSQL_DB}?driver=ODBC+Driver+17+for+SQL+Server"

# MinIO config
MINIO_ENDPOINT = "http://localhost:9000"  # nếu MinIO chạy trong WSL2
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "lakehouse"

# ==============================
# Lấy danh sách bảng từ Airflow Variable
# ==============================
TABLES_TO_EXTRACT = Variable.get("tables_to_extract", default_var=["dbo.DimCustomer", "dbo.DimProduct", "dbo.FactInternetSales"])
TABLES_TO_EXTRACT = [tuple(table.split('.')) for table in TABLES_TO_EXTRACT]

# ==============================
# Function: Clean dtype
# ==============================
def _coerce_chunk(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        s = df[col]

        if s.notna().sum() == 0:
            df[col] = s.astype("string")
            continue

        # UUID → string
        try:
            if s.map(lambda x: isinstance(x, uuid.UUID)).any():
                df[col] = s.astype(str)
                continue
        except Exception:
            pass

        # bytes → base64 string
        try:
            has_bytes = s.map(lambda x: isinstance(x, (bytes, bytearray))).any()
        except:
            has_bytes = False

        if has_bytes:
            df[col] = s.map(
                lambda x: base64.b64encode(x).decode("ascii")
                if isinstance(x, (bytes, bytearray))
                else str(x) if pd.notna(x) else None
            )
            continue

        # Decimal → string
        try:
            has_decimal = s.map(lambda x: isinstance(x, decimal.Decimal)).any()
        except:
            has_decimal = False

        if has_decimal:
            df[col] = s.map(lambda x: str(x) if isinstance(x, decimal.Decimal) else x)
            continue

        # object → UTF8 string
        if pd.api.types.is_object_dtype(s):
            df[col] = s.astype("string")
            continue

    return df


# ==============================
# Main extract function
# ==============================
def extract_dw_tables():
    print("Connecting to MSSQL...")
    engine = create_engine(mssql_url)

    print("Connecting to MinIO...")
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    for schema, table in TABLES_TO_EXTRACT:
        print(f"\n=== Extracting {schema}.{table} ===")

        query = f"SELECT * FROM [{schema}].[{table}]"
        chunks = pd.read_sql(query, engine, chunksize=100_000)

        buf = BytesIO()
        writer = None
        row_count = 0

        for chunk in chunks:
            chunk = _coerce_chunk(chunk)
            row_count += len(chunk)

            tbl_pa = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(buf, tbl_pa.schema, compression="snappy")
            writer.write_table(tbl_pa)

        # Finish file
        if writer is None:
            empty_table = pa.Table.from_pandas(pd.DataFrame(), preserve_index=False)
            writer = pq.ParquetWriter(buf, empty_table.schema, compression="snappy")
            writer.close()
        else:
            writer.close()

        buf.seek(0)

        # Upload to MinIO:
        key = f"bronze/adventureworksdw/{schema}/{table}/{table}.parquet"
        s3.upload_fileobj(buf, BUCKET_NAME, key)

        print(f"Uploaded s3://{BUCKET_NAME}/{key}  (rows={row_count})")


# ==============================
# Run
# ==============================
if __name__ == "__main__":
    extract_dw_tables()
    print("\nDONE!")
