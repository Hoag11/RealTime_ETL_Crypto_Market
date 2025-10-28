import logging
import sys
import traceback
from pathlib import Path

from pyspark.sql import SparkSession

# -------------------- Logging setup --------------------
log = logging.getLogger(__name__)
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s - %(message)s"))
    log.addHandler(handler)
log.setLevel(logging.INFO)

# -------------------- Import configs --------------------
SRC_DIR = Path(__file__).resolve()
for parent in SRC_DIR.parents:
    candidate = parent / "src"
    if candidate.exists():
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
        break

from core.config import get_settings

# -------------------- Load configs --------------------
settings = get_settings()
MINIO_ENDPOINT = settings.minio_endpoint.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = settings.minio_access_key
MINIO_SECRET_KEY = settings.minio_secret_key
MINIO_BUCKET_RAW_PRICE = settings.minio_bucket_raw_price

PG_HOST = settings.postgres_host
PG_PORT = settings.postgres_port
PG_USER = settings.postgres_user
PG_PASSWORD = settings.postgres_password
PG_DB = settings.postgres_db


# -------------------- Main logic --------------------
def main():
    log.info("Bắt đầu job dim_exchanges")

    spark = None
    try:
        # 1) Khởi tạo SparkSession
        log.info("Khởi tạo SparkSession với cấu hình MinIO S3A")
        spark = (
            SparkSession.builder.appName("dim_exchanges_job")
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
        log.info("SparkSession khởi tạo thành công")

        # 2) Đọc dữ liệu JSON từ MinIO (đường dẫn thực tế)
        path = f"s3a://{MINIO_BUCKET_RAW_PRICE}/exchange_list/*.json"
        log.info(f"Đọc dữ liệu từ: {path}")

        df = spark.read.option("multiLine", True).json(path)
        log.info(f"Số bản ghi nạp: {df.count()}")
        df.printSchema()

        # 3) Ghi thẳng vào Postgres (giữ nguyên logic gốc)
        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        log.info(f"Ghi dữ liệu vào: {jdbc_url}, bảng: dim_exchanges")

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

    except Exception as e:
        log.error("Job dim_exchanges thất bại")
        log.error(str(e))
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            log.info("Đóng SparkSession")
            spark.stop()
            log.info("Kết thúc job dim_exchanges")


if __name__ == "__main__":
    main()
