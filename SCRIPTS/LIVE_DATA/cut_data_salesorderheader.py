from io import BytesIO
import pandas as pd
import boto3

# ========== CẤU HÌNH MINIO / S3 ==========

# Nếu script chạy TRONG docker network cùng MinIO:
MINIO_ENDPOINT = "http://localhost:9000"
# Nếu chạy script ngoài host và MinIO expose ra localhost:9000 thì đổi thành:
# MINIO_ENDPOINT = "http://localhost:9000"

MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "lakehouse"

# File full salesorderheader hiện đang nằm trên MinIO
# -> CHỈNH LẠI đường dẫn này cho đúng file full của mày
SOURCE_KEY = "bronze/adventureworks/Sales/SalesOrderHeader/SalesOrderHeader.parquet"

# Prefix đích cho dữ liệu daily mới
# sẽ tạo ra: lakehouse/bronze/iceberg/Sales/SalesOrderHeader_daily/load_date=YYYY-MM-DD/...
TARGET_PREFIX = "bronze/adventureworks/Sales/SalesOrderHeader_daily"

# Tên cột ngày trong bảng
DATE_COLUMN = "OrderDate"  # hoặc "OrderDate" tùy đúng tên cột trong dữ liệu của mày


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def load_full_salesorderheader(s3):
    """
    Tải file Parquet full từ MinIO vào memory (BytesIO) rồi đọc vào pandas.
    Không lưu file ra ổ đĩa local.
    """
    print(f"[INFO] Đang tải s3://{BUCKET}/{SOURCE_KEY} ...")
    obj = s3.get_object(Bucket=BUCKET, Key=SOURCE_KEY)
    data = obj["Body"].read()

    buffer = BytesIO(data)
    df = pd.read_parquet(buffer)

    print(f"[INFO] Đã đọc {len(df)} dòng từ file full.")
    return df


def write_daily_to_minio(s3, df):
    """
    Chẻ df theo ngày (DATE_COLUMN) và upload từng phần lên MinIO
    theo dạng:
      bronze/iceberg/Sales/SalesOrderHeader_daily/load_date=YYYY-MM-DD/salesorderheader_YYYY-MM-DD.parquet
    """
    if DATE_COLUMN not in df.columns:
        raise KeyError(
            f"Không tìm thấy cột '{DATE_COLUMN}' trong dữ liệu! "
            f"Các cột đang có: {list(df.columns)}"
        )

    # convert sang datetime
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])

    unique_dates = sorted(df[DATE_COLUMN].dt.date.unique())
    print(f"[INFO] Tổng số ngày khác nhau: {len(unique_dates)}")

    for d in unique_dates:
        df_day = df[df[DATE_COLUMN].dt.date == d]
        if df_day.empty:
            continue

        # đường dẫn trên MinIO
        key_prefix = f"{TARGET_PREFIX}/load_date={d}"
        key = f"{key_prefix}/salesorderheader_{d}.parquet"

        # ghi vào buffer rồi upload
        buffer = BytesIO()
        df_day.to_parquet(buffer, index=False)
        buffer.seek(0)

        print(f"[UPLOAD] {d}: {len(df_day)} dòng -> s3://{BUCKET}/{key}")
        s3.upload_fileobj(buffer, BUCKET, key)

    print("[DONE] Đã chẻ xong & upload hết daily salesorderheader lên MinIO.")


def main():
    s3 = get_s3_client()
    df = load_full_salesorderheader(s3)
    write_daily_to_minio(s3, df)


if __name__ == "__main__":
    main()
