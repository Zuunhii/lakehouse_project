FROM python:3.11-alpine
WORKDIR /app
RUN pip install --no-cache-dir "minio>=7.2.5"
COPY create_buckets.py /app/create_buckets.py

ENV MINIO_ENDPOINT=http://minio:9000
ENV MINIO_ACCESS_KEY=minio
ENV MINIO_SECRET_KEY=minio123
ENV BUCKET_NAME=lakehouse

ENTRYPOINT ["python", "/app/create_buckets.py"]
