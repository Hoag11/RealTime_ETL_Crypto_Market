import sys
import traceback
import logging
from pyspark.sql import SparkSession

# -------------------- Logging setup --------------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s"
)
log = logging.getLogger("dim_exchanges")

# -------------------- Import configs --------------------
if "/configs" not in sys.path:
    sys.path.insert(0, "/configs")

try:
    import configs
except Exception:
    log.error(
        "Không thể import configs.py từ /configs. Kiểm tra volume mount và tên file."
    )
    raise

# -------------------- Load configs --------------------
MINIO_ENDPOINT = getattr(configs, "MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = getattr(configs, "MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = getattr(configs, "MINIO_SECRET_KEY", "")
MINIO_BUCKET_RAW_PRICE = getattr(configs, "MINIO_BUCKET_RAW_PRICE", "crypto-data-raw")

PG_HOST = getattr(configs, "PG_HOST", "postgres-bi")
PG_PORT = int(getattr(configs, "PG_PORT", 5432))
PG_USER = getattr(configs, "PG_USER", "postgres")
PG_PASSWORD = getattr(configs, "PG_PASSWORD", "postgres")
PG_DB = getattr(configs, "PG_DB", "warehouse")


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
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.master", "spark://spark-spark-1:7077")
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
