import logging
import sys
import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast, col, to_date

from pipelines import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("fact_price_history")

BUCKET_NAME = getattr(config, "MINIO_BUCKET_RAW_PRICE", "crypto-data-raw")
MINIO_ENDPOINT = config.MINIO_ENDPOINT
MINIO_ACCESS_KEY = config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = config.MINIO_SECRET_KEY
OBJECT_FOLDER = "historical"
PG_HOST = config.PG_HOST
PG_PORT = config.PG_PORT
PG_USER = config.PG_USER
PG_PASSWORD = config.PG_PASSWORD
PG_DB = config.PG_DB
PATH_OBJECT = f"s3a://{BUCKET_NAME}/{OBJECT_FOLDER}/*.json"


def main():
    spark = (
        SparkSession.builder.appName("Load Fact Price History")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "true" if MINIO_ENDPOINT.startswith("https") else "false",
        )
        .getOrCreate()
    )

    try:
        log.info("Khởi động SparkSession thành công")
        log.info("Đang đọc dữ liệu từ %s", PATH_OBJECT)
        df = spark.read.option("multiline", True).json(PATH_OBJECT)

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
        log.info("Đã chuẩn hóa dữ liệu, số bản ghi: %s", df_transformed.count())

        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        connection_props = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }

        log.info("Đang đọc bảng dim_dates từ PostgreSQL...")
        dim_dates = spark.read.jdbc(url=jdbc_url, table="dim_dates", properties=connection_props)

        log.info("Đang join với dim_dates...")
        df_final = (
            df_transformed.alias("t")
            .join(
                broadcast(dim_dates.alias("d")),
                F.col("t.date_only") == F.col("d.full_date"),
                "left",
            )
            .select(
                col("t.coin_id").alias("coin_id"),
                col("d.date_id").alias("date_id"),
                col("t.open").alias("open"),
                col("t.high").alias("high"),
                col("t.low").alias("low"),
                col("t.close").alias("close"),
                col("t.volume").alias("volume"),
                col("t.ingestion_timestamp").alias("ingestion_timestamp"),
            )
        )

        log.info("Join thành công, số bản ghi sau join: %s", df_final.count())
        log.info("Ghi dữ liệu vào bảng fact_price_history...")
        df_final.write.jdbc(
            url=jdbc_url,
            table="fact_price_history",
            mode="append",
            properties=connection_props,
        )
        log.info("Ghi dữ liệu vào fact_price_history thành công.")

    except Exception as exc:
        log.error("Job fact_price_history thất bại: %s", exc)
        traceback.print_exc()
        sys.exit(1)
    finally:
        log.info("Đóng SparkSession.")
        spark.stop()


if __name__ == "__main__":
    main()
