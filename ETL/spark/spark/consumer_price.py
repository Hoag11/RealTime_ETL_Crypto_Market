from typing import Any, Dict, Optional

import logging
import threading
import time

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

import configs

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server
except ImportError:  # pragma: no cover - exercised when optional dependency missing
    class CollectorRegistry:  # type: ignore
        def __init__(self, *args, **kwargs):
            self._metrics = {}

        def register(self, name, metric):
            self._metrics[name] = metric

        def get_sample_value(self, name):
            metric = self._metrics.get(name)
            return getattr(metric, "_value", None)

    class _FallbackMetric:  # pragma: no cover - only used in environments without deps
        def __init__(self, name, documentation, registry=None, **kwargs):
            self._value = 0.0
            if registry is not None and hasattr(registry, "register"):
                registry.register(name, self)

        def inc(self, amount=1):
            self._value += amount

        def set(self, value):
            self._value = value

    class Counter(_FallbackMetric):  # type: ignore
        pass

    class Gauge(_FallbackMetric):  # type: ignore
        pass

    def start_http_server(port, registry=None):  # type: ignore
        logging.getLogger(__name__).warning(
            "prometheus_client is not installed; metrics endpoint disabled."
        )

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
MINIO_BUCKET_RAW_PRICE = configs.MINIO_BUCKET_RAW_PRICE

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


METRICS_REGISTRY = CollectorRegistry()
STREAMED_RECORDS = Counter(
    "spark_stream_processed_records_total",
    "Total number of records processed by the Spark streaming job.",
    registry=METRICS_REGISTRY,
)
LAST_TRIGGER_DURATION_SECONDS = Gauge(
    "spark_stream_last_trigger_duration_seconds",
    "Duration of the latest Spark streaming trigger execution in seconds.",
    registry=METRICS_REGISTRY,
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


def parse_kafka_payload(raw_df):
    """Parse the Kafka payload using the predefined schema."""

    return (
        raw_df.selectExpr("CAST(value AS STRING)")
        .select(
            from_json(col("value"), main_schema, {"mode": "PERMISSIVE"}).alias(
                "parsed_data"
            )
        )
        .select("parsed_data.data", "parsed_data.info")
    )


def transform_coin_dataframe(parsed_df):
    """Transform the parsed dataframe into a normalized schema."""

    df_exploded = parsed_df.select(
        explode(col("data")).alias("coin_data"), col("info")
    ).select(
        col("coin_data.*"),
        col("info.coins_num").alias("total_coins"),
        col("info.time").alias("api_timestamp"),
    )

    return (
        df_exploded.withColumn(
            "price_usd_numeric", col("price_usd").cast(DoubleType())
        )
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
        .withColumn(
            "market_cap_usd_numeric", col("market_cap_usd").cast(DoubleType())
        )
        .withColumn("csupply_numeric", col("csupply").cast(DoubleType()))
        .withColumn("tsupply_numeric", col("tsupply").cast(DoubleType()))
        .withColumn("msupply_numeric", col("msupply").cast(DoubleType()))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("date", to_date(col("ingestion_timestamp")))
        .withWatermark("ingestion_timestamp", "10 minutes")
    )


def record_streaming_metrics(
    progress: Optional[Dict[str, Any]],
    *,
    records_metric: Counter = STREAMED_RECORDS,
    trigger_duration_metric: Gauge = LAST_TRIGGER_DURATION_SECONDS,
) -> None:
    """Update Prometheus metrics based on the query progress payload."""

    if not progress:
        return

    num_rows = progress.get("numInputRows")
    if isinstance(num_rows, int):
        records_metric.inc(num_rows)

    duration_ms = None
    duration_payload = progress.get("durationMs")
    if isinstance(duration_payload, dict):
        duration_ms = duration_payload.get("triggerExecution")

    if isinstance(duration_ms, (int, float)):
        trigger_duration_metric.set(duration_ms / 1000.0)


def start_metrics_server(port: int = 8000) -> None:
    """Expose Prometheus metrics for the streaming job."""

    start_http_server(port, registry=METRICS_REGISTRY)


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
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{KAFKA_SASL_USERNAME}' password='{KAFKA_SASL_PASSWORD}';",
        )
        .load()
    )

    df_parsed = parse_kafka_payload(df)

    df_processed = transform_coin_dataframe(df_parsed)

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

    start_metrics_server()

    logging.info(
        f"Streaming to MinIO started at s3a://{MINIO_BUCKET_RAW_PRICE}/raw-data"
    )

    # Theo dõi query
    def log_query_progress(query):
        while query.isActive:
            progress = query.lastProgress or {}
            record_streaming_metrics(progress)
            logging.info("Query status: %s", query.status)
            if progress:
                logging.info("Last query progress: %s", progress)
            time.sleep(60)

    threading.Thread(target=log_query_progress, args=(query,), daemon=True).start()

    query.awaitTermination()
