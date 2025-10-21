import sys, traceback, logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s"
)
log = logging.getLogger("fact_market_cap")

if "/configs" not in sys.path:
    sys.path.insert(0, "/configs")
import configs

BUCKET_NAME = getattr(configs, "MINIO_BUCKET_RAW_PRICE", "crypto-data-raw")
MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY = (
    configs.MINIO_ENDPOINT,
    configs.MINIO_ACCESS_KEY,
    configs.MINIO_SECRET_KEY,
)
PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB = (
    configs.PG_HOST,
    configs.PG_PORT,
    configs.PG_USER,
    configs.PG_PASSWORD,
    configs.PG_DB,
)

PATH_OBJECT = f"s3a://{BUCKET_NAME}/global_market/*.json"


def main():
    spark = None
    try:
        log.info("Khởi tạo SparkSession cho fact_market_cap")
        spark = (
            SparkSession.builder.appName("Load fact_market_cap")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )

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

        dim_dates = spark.read.jdbc(jdbc_url, "dim_dates", properties=pg_props).select(
            "date_id", F.to_date("full_date").alias("full_date")
        )
        fact_market_global = spark.read.jdbc(
            jdbc_url, "fact_market_global", properties=pg_props
        ).select("market_id", "date_id")

        log.info(
            "Chuyển struct total_market_cap thành dạng key-value (currency_id, value)"
        )
        df_cap = df.select(
            F.expr(
                "stack(65, "
                + ",".join(
                    [
                        f"'{c}', total_market_cap.{c}"
                        for c in df.select("total_market_cap.*").columns
                    ]
                )
                + ") as (currency_id, value)"
            ),
            F.col("updated_at").cast("timestamp"),
        ).withColumn("date_only", F.to_date("updated_at"))

        df_cap = (
            df_cap.join(dim_dates, F.col("date_only") == F.col("full_date"), "left")
            .join(fact_market_global, "date_id", "left")
            .select("market_id", "currency_id", "value")
        )

        df_cap.write.jdbc(
            jdbc_url, "fact_market_cap", mode="append", properties=pg_props
        )
        log.info("Ghi dữ liệu vào fact_market_cap thành công.")

    except Exception as e:
        log.error(f"Lỗi khi xử lý fact_market_cap: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            log.info("Đã đóng SparkSession.")


if __name__ == "__main__":
    main()
