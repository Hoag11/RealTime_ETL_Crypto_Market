from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode,
    expr,
    monotonically_increasing_id,
    current_timestamp,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)
import configs
import logging
import threading
import time

# MinIO configuration
MINIO_ENDPOINT = configs.MINIO_ENDPOINT
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
MINIO_BUCKET_RAW_BATCH = configs.MINIO_BUCKET_RAW_PRICE

# Postgres configuration
PG_HOST = configs.PG_HOST
PG_PORT = configs.PG_PORT
PG_DB = configs.PG_DB
PG_USER = configs.PG_USER
PG_PWD = configs.PG_PASSWORD

# JDBC URL and properties
jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
properties = {"user": PG_USER, "password": PG_PWD, "driver": "org.postgresql.Driver"}


def create_spark_session():
    spark = (
        SparkSession.builder.appName("BatchLoad")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.6,org.postgresql:postgresql:42.7.3",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_dim_exchanges():
    try:
        exchanges_df = (
            spark.read.json(
                f"s3a://{MINIO_BUCKET_RAW_BATCH}/exchanges_list_2025-09-28.json"
            )
            .withColumnRenamed("id", "exchange_id")
            .select("exchange_id", "name")
        )
        exchanges_df.write.format("jdbc").mode("overwrite").option(
            "url", jdbc_url
        ).option("dbtable", "dim_exchanges").options(**properties).save()
        logging.info("Successfully loaded dim_exchanges")
    except Exception as e:
        logging.error(f"Failed to load dim_exchanges: {str(e)}")


def load_fact_price_history():
    try:
        historical_df = (
            spark.read.json(f"s3a://{MINIO_BUCKET_RAW_BATCH}/historical_data.json")
            .withColumn(
                "date_id",
                expr("date_format(to_timestamp(datetime), 'yyyyMMdd')").cast("integer"),
            )
            .select("coin_id", "date_id", "open", "high", "low", "close", "volume")
        )
        historical_df.write.format("jdbc").mode("append").option(
            "url", jdbc_url
        ).option("dbtable", "fact_price_history").options(**properties).save()
        logging.info("Successfully loaded fact_price_history")
    except Exception as e:
        logging.error(f"Failed to load fact_price_history: {str(e)}")


def load_fact_market_global_and_related():
    try:
        global_market_df = (
            spark.read.json(
                f"s3a://{MINIO_BUCKET_RAW_BATCH}/global_market_2025-09-28.json"
            )
            .withColumn(
                "date_id",
                expr("date_format(to_timestamp(updated_at), 'yyyyMMdd')").cast(
                    "integer"
                ),
            )
            .withColumn("market_id", monotonically_increasing_id())
        )

        # fact_market_global
        global_market_df.select(
            "market_id",
            "date_id",
            "active_cryptocurrencies",
            "upcoming_icos",
            "ongoing_icos",
            "ended_icos",
            "markets",
            "market_cap_change_percentage_24h_usd",
            "updated_at",
        ).write.format("jdbc").mode("append").option("url", jdbc_url).option(
            "dbtable", "fact_market_global"
        ).options(**properties).save()

        # fact_market_cap
        market_cap_df = global_market_df.select(
            explode("total_market_cap").alias("currency_id", "value"), "market_id"
        )
        market_cap_df.write.format("jdbc").mode("append").option(
            "url", jdbc_url
        ).option("dbtable", "fact_market_cap").options(**properties).save()

        # fact_market_volume
        market_volume_df = global_market_df.select(
            explode("total_volume").alias("currency_id", "value"), "market_id"
        )
        market_volume_df.write.format("jdbc").mode("append").option(
            "url", jdbc_url
        ).option("dbtable", "fact_market_volume").options(**properties).save()

        # fact_market_cap_percentage
        market_cap_percentage_df = global_market_df.select(
            explode("market_cap_percentage").alias("coin_symbol", "percentage"),
            "market_id",
        )
        market_cap_percentage_df.write.format("jdbc").mode("append").option(
            "url", jdbc_url
        ).option("dbtable", "fact_market_cap_percentage").options(**properties).save()

        logging.info("Successfully loaded fact_market_global and related tables")
    except Exception as e:
        logging.error(f"Failed to load fact_market_global and related: {str(e)}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("batch_load.log")],
    )

    spark = create_spark_session()

    # Load batch data
    load_dim_exchanges()
    load_fact_price_history()
    load_fact_market_global_and_related()

    spark.stop()
    logging.info("Batch load completed")
