import os, io
from minio import Minio
from minio.error import S3Error

endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
secure = endpoint.startswith("https://")
endpoint = endpoint.replace("http://", "").replace("https://", "")

client = Minio(endpoint,
               access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
               secret_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
               secure=secure)

bucket = os.getenv("BUCKET_NAME", "lakehouse")

def ensure_bucket(name):
    if not client.bucket_exists(name):
        client.make_bucket(name)
        print(f"Created bucket {name}")
    else:
        print(f"Bucket {name} already exists")

def ensure_prefix(name, prefix):
    key = f"{prefix.strip('/')}/.keep"
    try:
        client.put_object(name, key, io.BytesIO(b" "), length=1)
    except S3Error as e:
        # object tồn tại thì bỏ qua
        pass
    print(f"Ensured s3://{name}/{prefix.strip('/')}/")

if __name__ == "__main__":
    ensure_bucket(bucket)
    for p in ("bronze", "silver", "gold"):
        ensure_prefix(bucket, p)
    print("MinIO bootstrap done.")
