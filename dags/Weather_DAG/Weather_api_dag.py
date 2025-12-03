import requests
import boto3
import io
import json
from datetime import datetime
from datetime import timedelta
import pandas as pd 

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.hooks.trino import TrinoHook

def _to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def _to_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
    
# === CONFIG MINIO ===
MINIO_ENDPOINT = "http://minio:9000"      # giống lúc mày test list bucket
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "lakehouse"

API_KEY = 'c44861ca77de3d13ff3d148323a81de1'
BASE_URL = "https://api.weatherstack.com/current"
CITY = "Hanoi, Vietnam"


def get_s3_client():
    s3 = boto3.client(
        "s3",
        endpoint_url = MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    return s3
# day la ham upload json to minio bronze
def upload_weather_json_to_minio(data: dict):
    s3 = get_s3_client()
    buckets = s3.list_buckets()
    
    # dam bao bucket ton tai
    if "Buckets" in buckets:
        names = [b["Name"] for b in buckets["Buckets"]]
        if BUCKET_NAME in names:
            print(f"Using existing bucket: {BUCKET_NAME} ")
        else:
            s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created.")
                   
    # tao ten file voi thoi gian hien tai
    ingest_date = datetime.utcnow().strftime("%Y-%m-%d")
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    
    object_name = f"bronze/weather/hanoi/raw/ingest_date={ingest_date}/weather_hanoi_{ts}.json"
    
    body = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_name,
        Body=body,
        ContentType="application/json"
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

def build_weather_row_for_silver(data: dict) -> dict:
    """
    Chuyển JSON lồng nhau từ Weatherstack thành 1 record phẳng (1 dòng bảng).
    Dùng json_normalize để xử lý chung, không fix cứng từng field.
    """
    #chuyen data tu json ve flat thang
    df = pd.json_normalize(data, sep="_")
    d_raw = df.iloc[0].to_dict()
    
    # 2) Chuẩn hoá key: thay '-' thành '_' để match với tên cột (us-epa-index -> us_epa_index)
    d = {k.replace("-", "_"): v for k, v in d_raw.items()}
    

    # 3) Các field là list -> lấy phần tử đầu
    weather_icons = d.get("current_weather_icons") or []
    if isinstance(weather_icons, list):
        weather_icon_url = weather_icons[0] if weather_icons else None
    else:
        weather_icon_url = weather_icons
        
    weather_descs = d.get("current_weather_descriptions") or []
    if isinstance(weather_descs, list):
        weather_desc = weather_descs[0] if weather_descs else None
    else:
        weather_desc = weather_descs
        
    #4) Các object lồng: astro, air_quality (sau khi normalize vẫn là dict)
    astro = d.get("current_astro")
    if not isinstance(astro, dict):
        astro = {}

    airq = d.get("current_air_quality")
    if not isinstance(airq, dict):
        airq = {}
    
    loc_time_raw = d.get("location_localtime")
    location_localtime = None
    if isinstance(loc_time_raw,str) and loc_time_raw.strip() != "":
            try:
                location_localtime = datetime.strptime(loc_time_raw, "%Y-%m-%d %H:%M")
            except ValueError:
                location_localtime = None
        
    
    # 5) Thời gian ingest + raw_json
    now_utc = datetime.utcnow().replace(microsecond=0)
    ingest_time_utc = now_utc
    raw_json_str = json.dumps(data, ensure_ascii=False)

    # 6) Build row đúng tên cột bảng
    row = {
        # id: dùng epoch microsecond cho đơn giản, vẫn đảm bảo unique theo lần chạy
        "id": int(now_utc.timestamp() * 1_000_000),

        "request_type": d.get("request_type"),
        "request_query": d.get("request_query"),
        "request_language": d.get("request_language"),
        "request_unit": d.get("request_unit"),

        "location_name": d.get("location_name"),
        "location_country": d.get("location_country"),
        "location_region": d.get("location_region"),
        "location_lat": _to_float(d.get("location_lat")),
        "location_lon": _to_float(d.get("location_lon")),
        "location_timezone_id": d.get("location_timezone_id"),
        "location_localtime": location_localtime,  
        "location_localtime_epoch": _to_int(d.get("location_localtime_epoch")),
        "location_utc_offset": _to_float(d.get("location_utc_offset")),

        "current_observation_time": d.get("current_observation_time"),
        "current_temperature": _to_float(d.get("current_temperature")),
        "current_weather_code": _to_int(d.get("current_weather_code")),
        "current_weather_icon_url": weather_icon_url,
        "current_weather_description": weather_desc,
        "current_wind_speed": _to_float(d.get("current_wind_speed")),
        "current_wind_degree": _to_int(d.get("current_wind_degree")),
        "current_wind_dir": d.get("current_wind_dir"),
        "current_pressure": _to_float(d.get("current_pressure")),
        "current_precip": _to_float(d.get("current_precip")),
        "current_humidity": _to_float(d.get("current_humidity")),
        "current_cloudcover": _to_float(d.get("current_cloudcover")),
        "current_feelslike": _to_float(d.get("current_feelslike")),
        "current_uv_index": _to_float(d.get("current_uv_index")),
        "current_visibility": _to_float(d.get("current_visibility")),
        "current_is_day": d.get("current_is_day"),

        "current_astro_sunrise": astro.get("sunrise"),
        "current_astro_sunset": astro.get("sunset"),
        "current_astro_moonrise": astro.get("moonrise"),
        "current_astro_moonset": astro.get("moonset"),
        "current_astro_moon_phase": astro.get("moon_phase"),
        "current_astro_moon_illumination": _to_float(astro.get("moon_illumination")),

        "current_air_quality_co": _to_float(airq.get("co")),
        "current_air_quality_no2": _to_float(airq.get("no2")),
        "current_air_quality_o3": _to_float(airq.get("o3")),
        "current_air_quality_so2": _to_float(airq.get("so2")),
        "current_air_quality_pm2_5": _to_float(airq.get("pm2_5")),
        "current_air_quality_pm10": _to_float(airq.get("pm10")),
        "current_air_quality_us_epa_index": _to_int(airq.get("us_epa_index")),
        "current_air_quality_gb_defra_index": _to_int(airq.get("gb_defra_index")),

        "ingest_time_utc": ingest_time_utc,
        "raw_json": raw_json_str,
    }

    return row
    

def write_row_to_iceberg_silver(record: dict):
    hook = TrinoHook(trino_conn_id="trino_default")

    columns = [
        "id",
        "request_type",
        "request_query",
        "request_language",
        "request_unit",
        "location_name",
        "location_country",
        "location_region",
        "location_lat",
        "location_lon",
        "location_timezone_id",
        "location_localtime",
        "location_localtime_epoch",
        "location_utc_offset",
        "current_observation_time",
        "current_temperature",
        "current_weather_code",
        "current_weather_icon_url",
        "current_weather_description",
        "current_wind_speed",
        "current_wind_degree",
        "current_wind_dir",
        "current_pressure",
        "current_precip",
        "current_humidity",
        "current_cloudcover",
        "current_feelslike",
        "current_uv_index",
        "current_visibility",
        "current_is_day",
        "current_astro_sunrise",
        "current_astro_sunset",
        "current_astro_moonrise",
        "current_astro_moonset",
        "current_astro_moon_phase",
        "current_astro_moon_illumination",
        "current_air_quality_co",
        "current_air_quality_no2",
        "current_air_quality_o3",
        "current_air_quality_so2",
        "current_air_quality_pm2_5",
        "current_air_quality_pm10",
        "current_air_quality_us_epa_index",
        "current_air_quality_gb_defra_index",
        "ingest_time_utc",
        "raw_json",
    ]

    row = tuple(record.get(col) for col in columns)

    hook.insert_rows(
        table="iceberg.silver.weather_observation",
        rows=[row],
        target_fields=columns,
        commit_every=1,
    )

    print("[silver] Inserted 1 row into iceberg.silver.weather_observation")



def ingest_hanoi_weather_to_minio():
    # 1) Gọi API
    data = fetch_hanoi_weather()

    # 2) Lưu raw JSON xuống bronze (MinIO)
    upload_weather_json_to_minio(data)

    # 3) Làm phẳng JSON thành record đúng schema silver
    record = build_weather_row_for_silver(data)

    # 4) Ghi record vào bảng Iceberg silver
    write_row_to_iceberg_silver(record)
    
default_args = {
    "owner": "data_engineer",
    # "retries" : 1,
    # "retry_delay": timedelta(minutes=2),
}
    
with DAG(
    dag_id="weather_hanoi_daily_to_minio",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025,11,17),
    catchup=False,
    tags=["weather", "minio", "bronze"],
) as dag:
    
    ingest_task = PythonOperator(
        task_id="ingest_hanoi_weather_to_minio",
        python_callable=ingest_hanoi_weather_to_minio,
    )

    
