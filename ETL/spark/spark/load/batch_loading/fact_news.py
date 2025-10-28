import logging
import traceback

from pyspark.sql import SparkSession, functions as F

from pipelines import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("fact_news")

BUCKET_NAME = getattr(config, "MINIO_BUCKET_RAW_NEWS", "crypto-news-raw")
MINIO_ENDPOINT = config.MINIO_ENDPOINT
MINIO_ACCESS_KEY = config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = config.MINIO_SECRET_KEY
PG_HOST = config.PG_HOST
PG_PORT = config.PG_PORT
PG_USER = config.PG_USER
PG_PASSWORD = config.PG_PASSWORD
PG_DB = config.PG_DB
PATH_OBJECT = f"s3a://{BUCKET_NAME}/*.json"


def _to_ts(col):
    """Chuyển epoch hoặc chuỗi thời gian sang timestamp."""
    return F.when(F.col(col).rlike("^[0-9]+$"), F.from_unixtime(F.col(col))).otherwise(
        F.to_timestamp(F.col(col))
    )


def main():
    spark = None
    try:
        log.info("🚀 Khởi tạo SparkSession cho fact_news ...")
        spark = (
            SparkSession.builder.appName("Load fact_news")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if MINIO_ENDPOINT.startswith("https") else "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        log.info(f"📥 Đọc dữ liệu JSON từ: {PATH_OBJECT}")
        df = spark.read.option("multiline", "true").json(PATH_OBJECT)
        if df.rdd.isEmpty():
            log.warning("Không có dữ liệu JSON trong bucket.")
            return

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

        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        pg_props = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }

        stage_table = "fact_news_stage"
        df_transformed.write.jdbc(jdbc_url, stage_table, mode="overwrite", properties=pg_props)
        log.info("✅ Ghi dữ liệu vào bảng tạm fact_news_stage thành công.")

        import psycopg2

        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB
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
            published_on = EXCLUDED.published_on,
            title = EXCLUDED.title,
            subtitle = EXCLUDED.subtitle,
            authors = EXCLUDED.authors,
            url = EXCLUDED.url,
            source_id = EXCLUDED.source_id,
            body = EXCLUDED.body,
            keywords = EXCLUDED.keywords,
            lang = EXCLUDED.lang,
            upvotes = EXCLUDED.upvotes,
            downvotes = EXCLUDED.downvotes,
            score = EXCLUDED.score,
            sentiment = EXCLUDED.sentiment,
            status = EXCLUDED.status,
            created_on = EXCLUDED.created_on,
            updated_on = EXCLUDED.updated_on,
            source_data = EXCLUDED.source_data,
            category_data = EXCLUDED.category_data;
        """
        cur.execute(merge_sql)
        cur.close()
        conn.close()
        log.info("✅ Merge dữ liệu vào fact_news thành công.")
    except Exception:
        log.error("❌ Lỗi trong quá trình load fact_news:\n%s", traceback.format_exc())
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
