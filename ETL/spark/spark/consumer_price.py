import logging
import sys
import threading
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_date, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
)

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

# Kafka configuration
KAFKA_BROKER = settings.kafka_brokers
KAFKA_TOPIC = settings.kafka_topic
KAFKA_SECURITY_PROCTOCOL = settings.kafka_security_protocol
KAFKA_SASL_MECHANISM = settings.kafka_sasl_mechanism
KAFKA_SASL_USERNAME = settings.kafka_sasl_username
KAFKA_SASL_PASSWORD = settings.kafka_sasl_password

# MinIO configuration
MINIO_ENDPOINT = settings.minio_endpoint
MINIO_ACCESS_KEY = settings.minio_access_key
MINIO_SECRET_KEY = settings.minio_secret_key
MINIO_BUCKET_RAW_PRICE = settings.minio_bucket_raw_price

# Schema cho từng coin record
coin_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("nameid", StringType(), True),
        StructField("rank", LongType(), True),
        StructField("price_usd", StringType(), True),  # Chú ý: trong JSON là string
        StructField("percent_change_24h", StringType(), True),  # String trong JSON
        StructField("percent_change_1h", StringType(), True),  # String trong JSON
        StructField("percent_change_7d", StringType(), True),  # String trong JSON
        StructField("price_btc", StringType(), True),  # String trong JSON
        StructField("market_cap_usd", StringType(), True),  # String trong JSON
        StructField("volume24", DoubleType(), True),
        StructField("volume24a", DoubleType(), True),
        StructField("csupply", StringType(), True),  # String trong JSON
        StructField("tsupply", StringType(), True),  # String trong JSON
        StructField("msupply", StringType(), True),  # String trong JSON, có thể null
    ]
)

# Schema cho info object
info_schema = StructType(
    [StructField("coins_num", LongType(), True), StructField("time", LongType(), True)]
)

# Schema tổng thể
main_schema = StructType(
    [
        StructField("data", ArrayType(coin_schema), True),
        StructField("info", info_schema, True),
    ]
)


def create_spark_session():
    spark = (
        SparkSession.builder.appName("RawIngestion")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.6",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.sql.streaming.checkpointLocation",
            f"s3a://{MINIO_BUCKET_RAW_PRICE}/checkpoint/raw-price-data",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    spark = create_spark_session()

    # Đọc stream từ Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(KAFKA_BROKER))
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
        .option("kafka.security.protocol", KAFKA_SECURITY_PROCTOCOL)
        .option(
            "kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{KAFKA_SASL_USERNAME}' password='{KAFKA_SASL_PASSWORD}';",
        )
        .load()
    )

    # Parse JSON với schema chính xác
    df_parsed = (
        df.selectExpr("CAST(value AS STRING)")
        .select(
            from_json(col("value"), main_schema, {"mode": "PERMISSIVE"}).alias(
                "parsed_data"
            )
        )
        .select("parsed_data.data", "parsed_data.info")
    )

    # Explode array để có từng coin riêng biệt
    df_exploded = df_parsed.select(
        explode(col("data")).alias("coin_data"), col("info")
    ).select(
        col("coin_data.*"),
        col("info.coins_num").alias("total_coins"),
        col("info.time").alias("api_timestamp"),
    )

    # Convert string fields to numeric và thêm timestamp
    df_processed = (
        df_exploded.withColumn("price_usd_numeric", col("price_usd").cast(DoubleType()))
        .withColumn(
            "percent_change_24h_numeric", col("percent_change_24h").cast(DoubleType())
        )
        .withColumn(
            "percent_change_1h_numeric", col("percent_change_1h").cast(DoubleType())
        )
        .withColumn(
            "percent_change_7d_numeric", col("percent_change_7d").cast(DoubleType())
        )
        .withColumn("price_btc_numeric", col("price_btc").cast(DoubleType()))
        .withColumn("market_cap_usd_numeric", col("market_cap_usd").cast(DoubleType()))
        .withColumn("csupply_numeric", col("csupply").cast(DoubleType()))
        .withColumn("tsupply_numeric", col("tsupply").cast(DoubleType()))
        .withColumn("msupply_numeric", col("msupply").cast(DoubleType()))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("date", to_date(col("ingestion_timestamp")))
        .withWatermark("ingestion_timestamp", "10 minutes")
    )

    # Lưu vào MinIO
    query = (
        df_processed.writeStream.format("parquet")
        .option("path", f"s3a://{MINIO_BUCKET_RAW_PRICE}/raw-data")
        .option(
            "checkpointLocation",
            f"s3a://{MINIO_BUCKET_RAW_PRICE}/checkpoint/raw-data",
        )
        .partitionBy("date")
        .trigger(processingTime="1 minute")
        .start()
    )

    logger.info(
        "Streaming to MinIO started at s3a://%s/raw-data", MINIO_BUCKET_RAW_PRICE
    )

    # Theo dõi query
    def log_query_progress(query):
        while query.isActive:
            logger.info("Query status: %s", query.status)
            time.sleep(60)

    threading.Thread(target=log_query_progress, args=(query,), daemon=True).start()

    query.awaitTermination()
