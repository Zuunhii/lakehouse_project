-- run-trino-sql/create-schemas.sql
-- Dùng S3A: cần đúng hive.s3.* trong iceberg.properties (bạn đã có)
CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location='s3a://lakehouse/bronze/');

CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location='s3a://lakehouse/silver/');

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location='s3a://lakehouse/gold/');
