import logging
import sys
import traceback

from pyspark.sql import SparkSession, functions as F

from pipelines import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("fact_market_global")

BUCKET_NAME = getattr(config, "MINIO_BUCKET_RAW_PRICE", "crypto-data-raw")
MINIO_ENDPOINT = config.MINIO_ENDPOINT
MINIO_ACCESS_KEY = config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = config.MINIO_SECRET_KEY
OBJECT_FOLDER = "global_market"
PG_HOST = config.PG_HOST
PG_PORT = config.PG_PORT
PG_USER = config.PG_USER
PG_PASSWORD = config.PG_PASSWORD
PG_DB = config.PG_DB
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
                "true" if MINIO_ENDPOINT.startswith("https") else "false",
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

    except Exception as exc:
        log.error("Job fact_market_global thất bại")
        log.error(str(exc))
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            log.info("Đóng SparkSession")
            spark.stop()


if __name__ == "__main__":
    main()
