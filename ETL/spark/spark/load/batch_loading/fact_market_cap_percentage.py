import logging
import sys
import traceback

from pyspark.sql import SparkSession, functions as F

from pipelines import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("fact_market_cap_percentage")

BUCKET_NAME = getattr(config, "MINIO_BUCKET_RAW_PRICE", "crypto-data-raw")
MINIO_ENDPOINT = config.MINIO_ENDPOINT
MINIO_ACCESS_KEY = config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = config.MINIO_SECRET_KEY
PG_HOST = config.PG_HOST
PG_PORT = config.PG_PORT
PG_USER = config.PG_USER
PG_PASSWORD = config.PG_PASSWORD
PG_DB = config.PG_DB
PATH_OBJECT = f"s3a://{BUCKET_NAME}/global_market/*.json"


def main():
    spark = None
    try:
        log.info("Khởi tạo SparkSession cho fact_market_cap_percentage")
        spark = (
            SparkSession.builder.appName("Load fact_market_cap_percentage")
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

        log.info(f"Đọc dữ liệu JSON từ: {PATH_OBJECT}")
        df = spark.read.option("multiline", "true").json(PATH_OBJECT)
        if df.rdd.isEmpty():
            log.warning("⚠️ Không tìm thấy dữ liệu JSON trong bucket.")
            return

        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        pg_props = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }

        dim_dates = spark.read.jdbc(jdbc_url, "dim_dates", properties=pg_props).select(
            "date_id", F.to_date("full_date").alias("full_date")
        )
        fact_market_global = spark.read.jdbc(
            jdbc_url, "fact_market_global", properties=pg_props
        ).select("market_id", "date_id")

        log.info("Chuyển struct market_cap_percentage thành key-value (coin_symbol, percentage)")
        cols = df.select("market_cap_percentage.*").columns
        stack_expr = "stack({}, {})".format(
            len(cols),
            ",".join(
                [f"'{c}', CAST(market_cap_percentage.{c} AS DOUBLE)" for c in cols]
            ),
        )

        df_pct = df.select(
            F.expr(stack_expr + " AS (coin_symbol, percentage)"),
            F.col("updated_at").cast("timestamp"),
        ).withColumn("date_only", F.to_date("updated_at"))

        df_pct = (
            df_pct.join(dim_dates, F.col("date_only") == F.col("full_date"), "left")
            .join(fact_market_global, "date_id", "left")
            .select("market_id", "coin_symbol", "percentage")
        )

        log.info("Ghi dữ liệu vào PostgreSQL (bảng: fact_market_cap_percentage)")
        df_pct.write.jdbc(
            jdbc_url, "fact_market_cap_percentage", mode="append", properties=pg_props
        )
        log.info("Ghi thành công fact_market_cap_percentage")

    except Exception as exc:
        log.error("Lỗi khi xử lý fact_market_cap_percentage: %s", exc)
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            log.info("Đã đóng SparkSession.")


if __name__ == "__main__":
    main()
