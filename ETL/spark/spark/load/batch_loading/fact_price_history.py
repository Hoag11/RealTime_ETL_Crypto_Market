import logging
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, to_date, broadcast

# -----------------------------
# Logging setup
# -----------------------------
log = logging.getLogger(__name__)
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s - %(message)s"))
    log.addHandler(handler)
log.setLevel(logging.INFO)

# -----------------------------
# Import config
# -----------------------------
SRC_DIR = Path(__file__).resolve()
for parent in SRC_DIR.parents:
    candidate = parent / "src"
    if candidate.exists():
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
        break

from core.config import get_settings

settings = get_settings()

# -----------------------------
# ENV / CONFIG
# -----------------------------
BUCKET_NAME = settings.minio_bucket_raw_price
MINIO_ENDPOINT = settings.minio_endpoint.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = settings.minio_access_key
MINIO_SECRET_KEY = settings.minio_secret_key
OBJECT_FOLDER = "historical"

PG_HOST = settings.postgres_host
PG_PORT = settings.postgres_port
PG_USER = settings.postgres_user
PG_PASSWORD = settings.postgres_password
PG_DB = settings.postgres_db

PATH_OBJECT = f"s3a://{BUCKET_NAME}/{OBJECT_FOLDER}/*.json"


# -----------------------------
# MAIN LOGIC
# -----------------------------
def main():
    spark = (
        SparkSession.builder.appName("Load Fact Price History")
        .config("spark.sql.session.timeZone", "UTC")
        # ✅ MinIO (S3A) configuration
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(settings.minio_endpoint.startswith("https://")).lower(),
        )
        .getOrCreate()
    )

    try:
        log.info("Khởi động SparkSession thành công")

        # ✅ Đọc dữ liệu từ MinIO
        input_path = PATH_OBJECT
        log.info(f"Đang đọc dữ liệu từ {input_path}")
        df = spark.read.option("multiline", True).json(input_path)

        # ✅ Chuẩn hóa schema
        df_transformed = (
            df.withColumn("coin_id", F.col("coin_id").cast("string"))
            .withColumn("datetime", F.to_timestamp("datetime"))
            .withColumn("date_only", to_date("datetime"))
            .withColumn("open", F.col("open").cast("double"))
            .withColumn("high", F.col("high").cast("double"))
            .withColumn("low", F.col("low").cast("double"))
            .withColumn("close", F.col("close").cast("double"))
            .withColumn("volume", F.col("volume").cast("double"))
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )
        log.info(f"Đã chuẩn hóa dữ liệu, số bản ghi: {df_transformed.count()}")

        # ✅ Kết nối PostgreSQL
        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        connection_props = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }

        # ✅ Đọc bảng dim_dates
        log.info("Đang đọc bảng dim_dates từ PostgreSQL...")
        dim_dates = spark.read.jdbc(
            url=jdbc_url, table="dim_dates", properties=connection_props
        )

        # ✅ Join với dim_dates
        log.info("Đang join với dim_dates...")
        df_final = (
            df_transformed.alias("t")
            .join(
                broadcast(dim_dates.alias("d")),
                F.col("t.date_only") == F.col("d.full_date"),
                "left",
            )
            .select(
                F.col("t.coin_id").alias("coin_id"),
                F.col("d.date_id").alias("date_id"),
                F.col("t.open").alias("open"),
                F.col("t.high").alias("high"),
                F.col("t.low").alias("low"),
                F.col("t.close").alias("close"),
                F.col("t.volume").alias("volume"),
                F.col("t.ingestion_timestamp").alias("ingestion_timestamp"),
            )
        )

        log.info(f"Join thành công, số bản ghi sau join: {df_final.count()}")

        # ✅ Ghi vào PostgreSQL
        log.info("Ghi dữ liệu vào bảng fact_price_history...")
        df_final.write.jdbc(
            url=jdbc_url,
            table="fact_price_history",
            mode="append",
            properties=connection_props,
        )
        log.info("Ghi dữ liệu vào fact_price_history thành công.")

    except Exception as e:
        log.error(f"Job fact_price_history thất bại: {e}")
        traceback.print_exc()
        sys.exit(1)

    finally:
        log.info("Đóng SparkSession.")
        spark.stop()


if __name__ == "__main__":
    main()
