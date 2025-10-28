import logging
import sys
import traceback
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SRC_DIR = Path(__file__).resolve()
for parent in SRC_DIR.parents:
    candidate = parent / "src"
    if candidate.exists():
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
        break

from core.config import get_settings

log = logging.getLogger(__name__)
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s - %(message)s"))
    log.addHandler(handler)
log.setLevel(logging.INFO)

settings = get_settings()

BUCKET_NAME = settings.minio_bucket_raw_price
MINIO_ENDPOINT = settings.minio_endpoint.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = settings.minio_access_key
MINIO_SECRET_KEY = settings.minio_secret_key
OBJECT_FOLDER = "global_market"

PG_HOST = settings.postgres_host
PG_PORT = settings.postgres_port
PG_USER = settings.postgres_user
PG_PASSWORD = settings.postgres_password
PG_DB = settings.postgres_db

PATH_OBJECT = f"s3a://{BUCKET_NAME}/{OBJECT_FOLDER}/*.json"


def main():
    spark = None
    try:
        log.info("Khởi tạo SparkSession cho fact_market_global")
        spark = (
            SparkSession.builder.appName("Load fact_market_global")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(settings.minio_endpoint.startswith("https://")).lower(),
            )
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        log.info(f"Đọc dữ liệu từ MinIO: {PATH_OBJECT}")
        df = spark.read.option("multiline", "true").json(PATH_OBJECT)
        if df.rdd.isEmpty():
            log.warning("Không có dữ liệu JSON trong bucket.")
            return

        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        pg_props = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }

        df_flat = df.select(
            F.col("active_cryptocurrencies").cast("int"),
            F.col("upcoming_icos").cast("int"),
            F.col("ongoing_icos").cast("int"),
            F.col("ended_icos").cast("int"),
            F.col("markets").cast("int"),
            F.col("market_cap_change_percentage_24h_usd").cast("double"),
            F.col("updated_at").cast("timestamp"),
        ).withColumn("date_only", F.to_date("updated_at"))

        dim_dates = spark.read.jdbc(jdbc_url, "dim_dates", properties=pg_props).select(
            "date_id", F.to_date("full_date").alias("full_date")
        )

        df_final = (
            df_flat.join(dim_dates, F.col("date_only") == F.col("full_date"), "left")
            .drop("full_date")
            .select(
                F.monotonically_increasing_id().alias("market_id"),
                "date_id",
                "active_cryptocurrencies",
                "upcoming_icos",
                "ongoing_icos",
                "ended_icos",
                "markets",
                "market_cap_change_percentage_24h_usd",
                "updated_at",
                F.current_timestamp().alias("ingestion_timestamp"),
            )
        )

        df_final.write.jdbc(
            jdbc_url, "fact_market_global", mode="append", properties=pg_props
        )
        log.info("Đã ghi dữ liệu vào bảng fact_market_global thành công.")

    except Exception as e:
        log.error("Job fact_market_global thất bại")
        log.error(str(e))
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            log.info("Đóng SparkSession")
            spark.stop()


if __name__ == "__main__":
    main()
