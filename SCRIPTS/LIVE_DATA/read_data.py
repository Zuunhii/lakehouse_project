import pandas as pd
import boto3
from io import BytesIO
import pyarrow.parquet as pq

# Cấu hình thông tin đăng nhập và endpoint MinIO
MINIO_ENDPOINT = "http://localhost:9000"  # Đảm bảo sử dụng đúng endpoint MinIO
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "lakehouse"

# Khởi tạo s3 client với thông tin đăng nhập
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Đường dẫn đến file Parquet của bạn trên MinIO
file_path = 'bronze/adventureworks/Sales/Faker_SalesOrderHeader_daily/load_date=2025-12-03/salesorderheader_fake_2025-12-03.parquet'

# Đọc file Parquet từ MinIO
response = s3_client.get_object(Bucket=BUCKET, Key=file_path)
parquet_data = response['Body'].read()

# Đọc dữ liệu từ BytesIO vào DataFrame
buffer = BytesIO(parquet_data)
df = pd.read_parquet(buffer, engine='pyarrow')

# Hiển thị một số dòng đầu tiên của dữ liệu
print(df.head())

# Kiểm tra thông tin của DataFrame (cột và kiểu dữ liệu)
print(df.info())
