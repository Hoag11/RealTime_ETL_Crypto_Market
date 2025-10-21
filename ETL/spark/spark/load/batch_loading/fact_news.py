import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s"
)
log = logging.getLogger("fact_news")

# ---------- Import configs ----------
if "/configs" not in sys.path:
    sys.path.insert(0, "/configs")
try:
    import configs
except Exception:
    log.error("Không thể import configs.py từ /configs.")
    sys.exit(1)

# ---------- Configs ----------
BUCKET_NAME = getattr(configs, "MINIO_BUCKET_RAW_NEWS", "crypto-news-raw")
MINIO_ENDPOINT = configs.MINIO_ENDPOINT
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
PG_HOST = configs.PG_HOST
PG_PORT = configs.PG_PORT
PG_USER = configs.PG_USER
PG_PASSWORD = configs.PG_PASSWORD
PG_DB = configs.PG_DB
PATH_OBJECT = f"s3a://{BUCKET_NAME}/*.json"


# ---------- Helper ----------
def _to_ts(col):
    """Chuyển epoch hoặc chuỗi thời gian sang timestamp."""
    return F.when(F.col(col).rlike("^[0-9]+$"), F.from_unixtime(F.col(col))).otherwise(
        F.to_timestamp(F.col(col))
    )


# ---------- Main ----------
def main():
    spark = None
    try:
        log.info("Khởi tạo SparkSession cho fact_news")
        spark = (
            SparkSession.builder.appName("Load fact_news")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        log.info(f"Đọc dữ liệu JSON từ: {PATH_OBJECT}")
        df = spark.read.option("multiline", "true").json(PATH_OBJECT)
        if df.rdd.isEmpty():
            log.warning("Không có dữ liệu JSON trong bucket.")
            return

        # ---------- Transform ----------
        df_transformed = df.select(
            F.col("type").cast("string").alias("type"),
            F.col("guid").cast("string").alias("guid"),
            _to_ts("published_on").alias("published_on"),
            F.col("title").cast("string").alias("title"),
            F.col("subtitle").cast("string").alias("subtitle"),
            F.col("authors").cast("string").alias("authors"),
            F.col("url").cast("string").alias("url"),
            F.col("source_id").cast("string").alias("source_id"),
            F.col("body").cast("string").alias("body"),
            F.col("keywords").cast("string").alias("keywords"),
            F.col("lang").cast("string").alias("lang"),
            F.col("upvotes").cast("int").alias("upvotes"),
            F.col("downvotes").cast("int").alias("downvotes"),
            F.col("score").cast("double").alias("score"),
            F.col("sentiment").cast("string").alias("sentiment"),
            F.col("status").cast("string").alias("status"),
            _to_ts("created_on").alias("created_on"),
            _to_ts("updated_on").alias("updated_on"),
            F.to_json(F.col("source_data")).alias("source_data"),
            F.to_json(F.col("category_data")).alias("category_data"),
        )

        columns_for_db = [
            "type",
            "guid",
            "published_on",
            "title",
            "subtitle",
            "authors",
            "url",
            "source_id",
            "body",
            "keywords",
            "lang",
            "upvotes",
            "downvotes",
            "score",
            "sentiment",
            "status",
            "created_on",
            "updated_on",
            "source_data",
            "category_data",
        ]

        missing = [c for c in columns_for_db if c not in df_transformed.columns]
        if missing:
            log.error(f"Thiếu cột trước khi ghi: {missing}")
            sys.exit(1)

        # ---------- Write ----------
        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        pg_props = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }

        log.info("Ghi dữ liệu vào PostgreSQL (bảng: fact_news)")
        (
            df_transformed.select(*columns_for_db)
            .withColumn("published_on", F.col("published_on").cast("timestamp"))
            .withColumn("created_on", F.col("created_on").cast("timestamp"))
            .withColumn("updated_on", F.col("updated_on").cast("timestamp"))
            .write.jdbc(jdbc_url, "fact_news", mode="append", properties=pg_props)
        )

        log.info("Ghi dữ liệu vào fact_news thành công.")

    except Exception as e:
        log.error(f"Lỗi khi xử lý fact_news: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            log.info("Đóng SparkSession.")
            spark.stop()


if __name__ == "__main__":
    main()
