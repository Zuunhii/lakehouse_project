import requests
from minio import Minio
import io
import json
from datetime import datetime

# === CONFIG MINIO ===
MINIO_ENDPOINT = "localhost:9000"      # giống lúc mày test list bucket
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False                   # True nếu có https
BUCKET_NAME = "lakehouse"

API_KEY = 'c44861ca77de3d13ff3d148323a81de1'
BASE_URL = "https://api.weatherstack.com/current"
CITY = "Hanoi, Vietnam"


def get_minio_client():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    return client

def upload_weather_json_to_minio(data: dict):
    client = get_minio_client()
    
    # dam bao bucket ton tai
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created.")
    else:
        print(f"Using bucket '{BUCKET_NAME}'")
               
    # tao ten file voi thoi gian hien tai
    ingest_date = datetime.utcnow().strftime("%Y-%m-%d")
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    object_name = f"bronze/weather/hanoi/raw/ingest_date={ingest_date}/weather_hanoi_{ts}.json"
    
    # chuyen dict thanh byte stream
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
    stream = io.BytesIO(json_bytes)
    
    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=stream,
        length=len(json_bytes),
        content_type="application/json"
    )
    print(f"Uploaded data to MinIO at '{object_name}'")
def fetch_hanoi_weather():
    print("Fetching weather data for Hanoi...")
    params = {
        'access_key': API_KEY,
        'query': CITY
    }
    req = requests.get(BASE_URL, params=params, timeout=10) # cau lenh keo API ve
    req.raise_for_status() # kiem tra loi http
    data = req.json() # chuyen du lieu thanh json
    if data.get("success") is False:
        raise Exception(data.get("error"))
    
    print("Data fetched successfully.")
    return data


def main():
    data = fetch_hanoi_weather()
    upload_weather_json_to_minio(data)
    
if __name__ == "__main__":
    main()