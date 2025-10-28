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
PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB = (
    settings.postgres_host,
    settings.postgres_port,
    settings.postgres_user,
    settings.postgres_password,
    settings.postgres_db,
)
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
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )

        # ------------------ ĐỌC DỮ LIỆU ------------------ #
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

        # ------------------ ĐỌC DIM ------------------ #
        dim_dates = spark.read.jdbc(jdbc_url, "dim_dates", properties=pg_props).select(
            "date_id", F.to_date("full_date").alias("full_date")
        )
        fact_market_global = spark.read.jdbc(
            jdbc_url, "fact_market_global", properties=pg_props
        ).select("market_id", "date_id")

        # ------------------ CHUYỂN STRUCT -> KV ------------------ #
        log.info(
            "Chuyển struct market_cap_percentage thành key-value (coin_symbol, percentage)"
        )
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

        # ------------------ JOIN DATE + MARKET ------------------ #
        df_pct = (
            df_pct.join(dim_dates, F.col("date_only") == F.col("full_date"), "left")
            .join(fact_market_global, "date_id", "left")
            .select("market_id", "coin_symbol", "percentage")
        )

        # ------------------ GHI DỮ LIỆU ------------------ #
        log.info("Ghi dữ liệu vào PostgreSQL (bảng: fact_market_cap_percentage)")
        df_pct.write.jdbc(
            jdbc_url, "fact_market_cap_percentage", mode="append", properties=pg_props
        )
        log.info("Ghi thành công fact_market_cap_percentage")

    except Exception as e:
        log.error(f"Lỗi khi xử lý fact_market_cap_percentage: {e}")
        traceback.print_exc()
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            log.info("Đã đóng SparkSession.")


if __name__ == "__main__":
    main()
