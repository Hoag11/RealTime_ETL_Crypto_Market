import sys
import traceback
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

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


def _to_ts(col):
    """Chuyển epoch hoặc chuỗi thời gian sang timestamp."""
    return F.when(F.col(col).rlike("^[0-9]+$"), F.from_unixtime(F.col(col))).otherwise(
        F.to_timestamp(F.col(col))
    )


def main():
    spark = None
    try:
        log.info("Khởi tạo SparkSession cho fact_news ...")
        spark = (
            SparkSession.builder.appName("Load fact_news")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.master", "spark://spark-spark-1:7077")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        log.info(f"Đọc dữ liệu JSON từ: {PATH_OBJECT}")
        df = spark.read.option("multiline", "true").json(PATH_OBJECT)
        if df.rdd.isEmpty():
            log.warning("Không có dữ liệu JSON trong bucket.")
            return

        # ---------- Transform ----------
        def limit(col, n=200):
            return F.substring(F.col(col).cast("string"), 1, n)

        df_transformed = df.select(
            limit("type").alias("type"),
            limit("guid", 200).alias("guid"),
            _to_ts("published_on").alias("published_on"),
            limit("title", 500).alias("title"),
            limit("subtitle", 300).alias("subtitle"),
            limit("authors", 200).alias("authors"),
            limit("url", 500).alias("url"),
            limit("source_id").alias("source_id"),
            F.col("body").cast("string").alias("body"),
            limit("keywords", 500).alias("keywords"),
            limit("lang").alias("lang"),
            F.col("upvotes").cast("int").alias("upvotes"),
            F.col("downvotes").cast("int").alias("downvotes"),
            F.col("score").cast("double").alias("score"),
            limit("sentiment").alias("sentiment"),
            limit("status").alias("status"),
            _to_ts("created_on").alias("created_on"),
            _to_ts("updated_on").alias("updated_on"),
            F.to_json(F.col("source_data")).alias("source_data_str"),
            F.to_json(F.col("category_data")).alias("category_data_str"),
        )

        # ============================================================
        #  DEDUP — GIỮ BẢN MỚI NHẤT THEO GUID (updated_on mới nhất)
        # ============================================================
        w = Window.partitionBy("guid").orderBy(
            F.col("updated_on").desc_nulls_last()
        )

        df_dedup = df_transformed.withColumn(
            "rn", F.row_number().over(w)
        ).filter("rn = 1").drop("rn")

        log.info(f"Tổng số record sau dedup: {df_dedup.count()}")
        # ============================================================

        # ---------- Ghi vào bảng stage ----------
        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        pg_props = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }

        stage_table = "fact_news_stage"

        df_dedup.write.jdbc(
            jdbc_url, stage_table, mode="overwrite", properties=pg_props
        )
        log.info("Ghi dữ liệu vào bảng tạm fact_news_stage thành công.")

        # ---------- Merge vào fact_news ----------
        import psycopg2

        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER,
            password=PG_PASSWORD, dbname=PG_DB
        )
        conn.autocommit = True
        cur = conn.cursor()

        merge_sql = """
        INSERT INTO fact_news (
            type, guid, published_on, title, subtitle, authors, url, source_id, body,
            keywords, lang, upvotes, downvotes, score, sentiment, status,
            created_on, updated_on, source_data, category_data
        )
        SELECT
            type,
            guid,
            published_on::timestamp,
            title,
            subtitle,
            authors,
            url,
            source_id,
            body,
            keywords,
            lang,
            upvotes,
            downvotes,
            score,
            sentiment,
            status,
            created_on::timestamp,
            updated_on::timestamp,
            source_data_str::jsonb,
            category_data_str::jsonb
        FROM fact_news_stage
        ON CONFLICT (guid) DO UPDATE SET
            title = EXCLUDED.title,
            subtitle = EXCLUDED.subtitle,
            authors = EXCLUDED.authors,
            url = EXCLUDED.url,
            body = EXCLUDED.body,
            keywords = EXCLUDED.keywords,
            sentiment = EXCLUDED.sentiment,
            status = EXCLUDED.status,
            updated_on = EXCLUDED.updated_on,
            source_data = EXCLUDED.source_data,
            category_data = EXCLUDED.category_data;
        """
        cur.execute(merge_sql)
        cur.close()
        conn.close()

        log.info("Merge dữ liệu vào fact_news thành công!")

    except Exception as e:
        log.error(f" Lỗi khi xử lý fact_news: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            log.info("Đóng SparkSession.")
            spark.stop()


if __name__ == "__main__":
    main()
