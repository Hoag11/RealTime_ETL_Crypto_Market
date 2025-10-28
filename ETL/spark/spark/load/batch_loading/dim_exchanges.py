import logging
import sys
import traceback

from pyspark.sql import SparkSession

from pipelines import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("dim_exchanges")

MINIO_ENDPOINT = config.MINIO_ENDPOINT
MINIO_ACCESS_KEY = config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = config.MINIO_SECRET_KEY
MINIO_BUCKET_RAW_PRICE = getattr(config, "MINIO_BUCKET_RAW_PRICE", "crypto-data-raw")
PG_HOST = config.PG_HOST
PG_PORT = config.PG_PORT
PG_USER = config.PG_USER
PG_PASSWORD = config.PG_PASSWORD
PG_DB = config.PG_DB


def main():
    log.info("Bắt đầu job dim_exchanges")
    spark = None
    try:
        log.info("Khởi tạo SparkSession với cấu hình MinIO S3A")
        spark = (
            SparkSession.builder.appName("dim_exchanges_job")
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
        log.info("SparkSession khởi tạo thành công")

        path = f"s3a://{MINIO_BUCKET_RAW_PRICE}/exchange_list/*.json"
        log.info("Đọc dữ liệu từ: %s", path)

        df = spark.read.option("multiLine", True).json(path)
        log.info("Số bản ghi nạp: %s", df.count())
        df.printSchema()

        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        log.info("Ghi dữ liệu vào: %s, bảng: dim_exchanges", jdbc_url)

        (
            df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", "dim_exchanges")
            .option("user", PG_USER)
            .option("password", PG_PASSWORD)
            .mode("overwrite")
            .save()
        )

        log.info("Hoàn tất ghi bảng dim_exchanges vào Postgres-BI")

    except Exception as exc:
        log.error("Job dim_exchanges thất bại")
        log.error(str(exc))
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            log.info("Đóng SparkSession")
            spark.stop()
            log.info("Kết thúc job dim_exchanges")


if __name__ == "__main__":
    main()
