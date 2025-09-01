from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from configs import configs
import logging
import threading
import time

# Kafka configuration
KAFKA_BROKER = configs.KAKFA_BROKER
KAFKA_TOPIC = configs.KAFKA_TOPIC
KAFKA_SECURITY_PROCTOCOL = configs.KAFKA_SECURITY_PROCTOCOL
KAFKA_SASL_MECHANISM = configs.KAFKA_SASL_MECHANISM
KAFKA_SASL_USERNAME = configs.KAFKA_SASL_USERNAME
KAFKA_SASL_PASSWORD = configs.KAFKA_SASL_PASSWORD

# MinIO configuration
MINIO_ENDPOINT = configs.MINIO_ENDPOINT
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
MINIO_BUCKET_RAW_NEWS = configs.MINIO_BUCKET_RAW_NEWS

# Schema
schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("nameid", StringType(), True),
        StructField("rank", LongType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("percent_change_24h", DoubleType(), True),
        StructField("percent_change_1h", DoubleType(), True),
        StructField("percent_change_7d", DoubleType(), True),
        StructField("price_btc", DoubleType(), True),
        StructField("market_cap_usd", DoubleType(), True),
        StructField("volume24", DoubleType(), True),
        StructField("volume24a", DoubleType(), True),
        StructField("csupply", DoubleType(), True),
        StructField("tsupply", DoubleType(), True),
        StructField("msupply", DoubleType(), True),
    ]
)


def create_spark_session():
    spark = (
        SparkSession.builder.appName("RawIngestion")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.6,org.scala-lang:scala-library:2.12.15",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.sql.streaming.checkpointLocation",
            f"s3a://{MINIO_BUCKET_RAW_NEWS}/checkpoint/raw-news-data",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

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
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{
                KAFKA_SASL_USERNAME
            }' password='{KAFKA_SASL_PASSWORD}';",
        )
        .load()
    )

    # Parse JSON
    df_parsed = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema, {"mode": "PERMISSIVE"}).alias("data"))
        .select("data.*")
    )

    # Thêm timestamp và date
    df_ts = (
        df_parsed.withColumn("timestamp", current_timestamp())
        .withColumn("date", to_date(col("timestamp")))
        .withWatermark("timestamp", "10 minutes")
    )

    # Lưu vào MinIO
    query = (
        df_ts.writeStream.format("parquet")
        .option("path", f"s3a://{MINIO_BUCKET_RAW_NEWS}/raw-news-data")
        .option(
            "checkpointLocation",
            f"s3a://{MINIO_BUCKET_RAW_NEWS}/checkpoint/raw-news-data",
        )
        .partitionBy("date")
        .trigger(processingTime="1 minute")
        .start()
    )

    logging.info(
        f"Streaming to MinIO started at s3a://{MINIO_BUCKET_RAW_NEWS}/raw-news-data"
    )

    # Theo dõi query
    def log_query_progress(query):
        while query.isActive:
            logging.info(f"Query status: {query.status}")
            time.sleep(60)

    threading.Thread(target=log_query_progress, args=(query,), daemon=True).start()

    query.awaitTermination()
