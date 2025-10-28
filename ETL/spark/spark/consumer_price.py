from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, current_timestamp, to_date
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pipelines import config as configs

import logging
import threading
import time

KAFKA_BROKER = configs.KAKFA_BROKER
KAFKA_TOPIC = configs.KAFKA_TOPIC
KAFKA_SECURITY_PROCTOCOL = configs.KAFKA_SECURITY_PROCTOCOL
KAFKA_SASL_MECHANISM = configs.KAFKA_SASL_MECHANISM
KAFKA_SASL_USERNAME = configs.KAFKA_SASL_USERNAME
KAFKA_SASL_PASSWORD = configs.KAFKA_SASL_PASSWORD

MINIO_ENDPOINT = configs.MINIO_ENDPOINT
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
MINIO_BUCKET_RAW_PRICE = configs.MINIO_BUCKET_RAW_PRICE

coin_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("nameid", StringType(), True),
        StructField("rank", LongType(), True),
        StructField("price_usd", StringType(), True),
        StructField("percent_change_24h", StringType(), True),
        StructField("percent_change_1h", StringType(), True),
        StructField("percent_change_7d", StringType(), True),
        StructField("price_btc", StringType(), True),
        StructField("market_cap_usd", StringType(), True),
        StructField("volume24", DoubleType(), True),
        StructField("volume24a", DoubleType(), True),
        StructField("csupply", StringType(), True),
        StructField("tsupply", StringType(), True),
        StructField("msupply", StringType(), True),
    ]
)

info_schema = StructType(
    [StructField("coins_num", LongType(), True), StructField("time", LongType(), True)]
)

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
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "true" if MINIO_ENDPOINT.startswith("https") else "false",
        )
        .config(
            "spark.sql.streaming.checkpointLocation",
            f"s3a://{MINIO_BUCKET_RAW_PRICE}/checkpoint/raw-price-data",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    spark = create_spark_session()

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

    df_parsed = (
        df.selectExpr("CAST(value AS STRING)")
        .select(
            from_json(col("value"), main_schema, {"mode": "PERMISSIVE"}).alias("parsed_data")
        )
        .select("parsed_data.data", "parsed_data.info")
    )

    df_exploded = df_parsed.select(explode("data").alias("coin"), "info")

    df_flat = df_exploded.select(
        col("coin.id").alias("coin_id"),
        col("coin.name").alias("coin_name"),
        col("coin.symbol").alias("coin_symbol"),
        col("coin.nameid").alias("coin_nameid"),
        col("coin.rank").cast("integer").alias("rank"),
        col("coin.price_usd").cast("double").alias("price_usd"),
        col("coin.percent_change_24h").cast("double").alias("percent_change_24h"),
        col("coin.percent_change_1h").cast("double").alias("percent_change_1h"),
        col("coin.percent_change_7d").cast("double").alias("percent_change_7d"),
        col("coin.price_btc").cast("double").alias("price_btc"),
        col("coin.market_cap_usd").cast("double").alias("market_cap_usd"),
        col("coin.volume24").cast("double").alias("volume24"),
        col("coin.volume24a").cast("double").alias("volume24a"),
        col("coin.csupply").cast("double").alias("csupply"),
        col("coin.tsupply").cast("double").alias("tsupply"),
        col("coin.msupply").cast("double").alias("msupply"),
        col("info.coins_num").alias("coins_num"),
        col("info.time").alias("info_time"),
        current_timestamp().alias("ingestion_timestamp"),
        to_date(current_timestamp()).alias("ingestion_date"),
    )

    def start_stream():
        query = (
            df_flat.writeStream.format("parquet")
            .option("path", f"s3a://{MINIO_BUCKET_RAW_PRICE}/raw-data")
            .option("checkpointLocation", f"s3a://{MINIO_BUCKET_RAW_PRICE}/checkpoint")
            .outputMode("append")
            .start()
        )
        query.awaitTermination()

    thread = threading.Thread(target=start_stream)
    thread.start()

    try:
        while thread.is_alive():
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("Stopping streaming job...")
    finally:
        spark.streams.active[0].stop()
        spark.stop()
