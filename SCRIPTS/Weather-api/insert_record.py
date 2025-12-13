from minio import Minio
from API_request import mock_fetch_data


client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
) 

bucket_name = "lakehouse" 

   
   