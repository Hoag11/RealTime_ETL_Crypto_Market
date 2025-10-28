import logging
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

SRC_DIR = Path(__file__).resolve()
for parent in SRC_DIR.parents:
    candidate = parent / "src"
    if candidate.exists():
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
        break

from core.config import get_settings

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

settings = get_settings()

MINIO_ENDPOINT = settings.minio_endpoint
MINIO_HOST = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
MINIO_SECURE = MINIO_ENDPOINT.startswith("https://")

bucket = settings.minio_bucket_raw_price.rstrip("/")
RAW_PATH = f"s3a://{bucket}/raw-data/"
CHECKPOINT_PATH = f"s3a://{bucket}/_checkpoints/fact_coin_metrics_stream/"
POSTGRES_URL = (
    f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/"
    f"{settings.postgres_db}"
)
POSTGRES_PROPS = {
    "user": settings.postgres_user,
    "password": settings.postgres_password,
    "driver": "org.postgresql.Driver",
}

# 🧩 Schema gốc (không dùng *_numeric nữa)
parquet_schema = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("symbol", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("nameid", T.StringType()),
        T.StructField("rank", T.LongType()),
        T.StructField("price_usd", T.StringType()),
        T.StructField("percent_change_24h", T.StringType()),
        T.StructField("percent_change_1h", T.StringType()),
        T.StructField("percent_change_7d", T.StringType()),
        T.StructField("price_btc", T.StringType()),
        T.StructField("market_cap_usd", T.StringType()),
        T.StructField("volume24", T.DoubleType()),
        T.StructField("volume24a", T.DoubleType()),
        T.StructField("csupply", T.StringType()),
        T.StructField("tsupply", T.StringType()),
        T.StructField("msupply", T.StringType()),
        T.StructField("total_coins", T.LongType()),
        T.StructField("api_timestamp", T.LongType()),
        T.StructField("ingestion_timestamp", T.TimestampType()),
    ]
)


def start_spark():
    spark = (
        SparkSession.builder.appName("StreamLoad_FactCoinMetrics")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_HOST)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MINIO_SECURE).lower())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_batch(df, batch_id):
    if df.isEmpty():
        logger.info("Batch %s: No data to process.", batch_id)
        return

    spark = df.sparkSession

    # 1️⃣ Đọc dim_dates để map sang date_id
    dim_dates = spark.read.jdbc(
        POSTGRES_URL, "dim_dates", properties=POSTGRES_PROPS
    ).select("date_id", F.to_date("full_date").alias("full_date"))

    # 2️⃣ Chuẩn hoá & ép kiểu
    df_casted = (
        df.withColumn("price_usd", F.col("price_usd").cast("double"))
        .withColumn("percent_change_1h", F.col("percent_change_1h").cast("double"))
        .withColumn("percent_change_24h", F.col("percent_change_24h").cast("double"))
        .withColumn("percent_change_7d", F.col("percent_change_7d").cast("double"))
        .withColumn("market_cap_usd", F.col("market_cap_usd").cast("double"))
        .withColumn("volume24", F.col("volume24").cast("double"))
        .withColumn("volume24a", F.col("volume24a").cast("double"))
        .withColumn("csupply", F.col("csupply").cast("double"))
        .withColumn("tsupply", F.col("tsupply").cast("double"))
        .withColumn("msupply", F.col("msupply").cast("double"))
        .withColumn(
            "api_timestamp", F.from_unixtime(F.col("api_timestamp")).cast("timestamp")
        )
        .withColumn("date_only", F.to_date("api_timestamp"))
    )

    # 3️⃣ Join dim_dates để lấy date_id
    df_joined = df_casted.join(
        dim_dates, F.col("date_only") == F.col("full_date"), "left"
    ).withColumn(
        "date_id",
        F.when(F.col("date_id").isNotNull(), F.col("date_id")).otherwise(
            F.date_format("date_only", "yyyyMMdd").cast("int")
        ),
    )

    # 4️⃣ Chọn đúng schema theo fact_coin_metrics
    df_final = df_joined.filter(F.col("date_id").isNotNull()).select(
        F.col("nameid").alias("coin_id"),
        "date_id",
        "price_usd",
        "percent_change_1h",
        "percent_change_24h",
        "percent_change_7d",
        "market_cap_usd",
        "volume24",
        "volume24a",
        "csupply",
        "tsupply",
        "msupply",
        "api_timestamp",
        F.current_timestamp().alias("ingestion_timestamp"),
    )

    # 5️⃣ Dedup trùng coin_id + date_id (chỉ giữ bản ghi mới nhất)
    window_spec = Window.partitionBy("coin_id", "api_timestamp").orderBy(
        F.col("api_timestamp").desc()
    )
    df_dedup = (
        df_final.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    record_count = df_dedup.count()
    if record_count == 0:
        logger.info("Batch %s: No valid records after deduplication.", batch_id)
        return

    logger.info(
        "Batch %s: Writing %s deduplicated rows to stage table...",
        batch_id,
        record_count,
    )

    # 6️⃣ Ghi vào bảng stage
    STAGE_TABLE = "fact_coin_metrics_stage"
    df_dedup.write.jdbc(POSTGRES_URL, STAGE_TABLE, "overwrite", POSTGRES_PROPS)

    # 7️⃣ Merge dữ liệu từ stage → fact_coin_metrics
    merge_query = """
        INSERT INTO fact_coin_metrics AS fcm
        SELECT * FROM fact_coin_metrics_stage
        ON CONFLICT (coin_id, api_timestamp)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            percent_change_1h = EXCLUDED.percent_change_1h,
            percent_change_24h = EXCLUDED.percent_change_24h,
            percent_change_7d = EXCLUDED.percent_change_7d,
            market_cap_usd = EXCLUDED.market_cap_usd,
            volume24 = EXCLUDED.volume24,
            volume24a = EXCLUDED.volume24a,
            csupply = EXCLUDED.csupply,
            tsupply = EXCLUDED.tsupply,
            msupply = EXCLUDED.msupply,
            api_timestamp = EXCLUDED.api_timestamp,
            ingestion_timestamp = EXCLUDED.ingestion_timestamp;
    """

    # 8️⃣ Kết nối PostgreSQL để chạy merge
    try:
        conn = psycopg2.connect(
            host=settings.postgres_host,
            database=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(merge_query)
        conn.close()
        logger.info("✅ Batch %s: Upsert completed successfully.", batch_id)
    except Exception as e:
        logger.error("❌ Upsert failed for batch %s: %s", batch_id, e)
    finally:
        # 9️⃣ Dọn bảng stage sau mỗi batch
        try:
            conn = psycopg2.connect(
                host=settings.postgres_host,
                database=settings.postgres_db,
                user=settings.postgres_user,
                password=settings.postgres_password,
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE fact_coin_metrics_stage;"))
            conn.close()
            logger.info("🧹 Stage table truncated after merge.")
        except Exception as e:
            logger.warning("⚠️ Could not truncate stage table: %s", e)


def main(spark=None):
    spark = spark or start_spark()

    logger.info("🔄 Reading streaming data from %s ...", RAW_PATH)
    df_stream = spark.readStream.schema(parquet_schema).parquet(f"{RAW_PATH}date=*")

    query = (
        df_stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="1 minute")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    spark_session = None
    try:
        spark_session = start_spark()
        logger.info("Spark session created. Starting streaming pipeline.")
        main(spark_session)
    except Exception as e:
        logger.error("Spark streaming job failed: %s", e)
        raise
    finally:
        if spark_session:
            spark_session.stop()
            logger.info("SparkSession stopped.")
