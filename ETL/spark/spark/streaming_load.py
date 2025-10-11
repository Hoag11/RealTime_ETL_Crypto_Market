from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
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
MINIO_BUCKET_RAW_PRICE = configs.MINIO_BUCKET_RAW_PRICE

# Postgres configuration
PG_HOST = configs.PG_HOST
PG_PORT = configs.PG_PORT
PG_DB = configs.PG_DB
PG_USER = configs.PG_USER
PG_PWD = configs.PG_PASSWORD

# JDBC URL and properties
jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
properties = {"user": PG_USER, "password": PG_PWD, "driver": "org.postgresql.Driver"}

# Schema for parquetreader.csv
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
        StructField("api_timestamp", LongType(), True),
        StructField("ingestion_timestamp", StringType(), True),
    ]
)


def create_spark_session():
    spark = (
        SparkSession.builder.appName("RawIngestion")
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
        .config(
            "spark.sql.streaming.checkpointLocation",
            f"s3a://{MINIO_BUCKET_RAW_PRICE}/checkpoint/raw-price-data",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def upsert_dim_coins(batch_df, batch_id):
    try:
        # Cast StringType to DoubleType for numeric fields
        batch_df = (
            batch_df.withColumn("price_usd", col("price_usd").cast("double"))
            .withColumn("percent_change_24h", col("percent_change_24h").cast("double"))
            .withColumn("percent_change_1h", col("percent_change_1h").cast("double"))
            .withColumn("percent_change_7d", col("percent_change_7d").cast("double"))
            .withColumn("price_btc", col("price_btc").cast("double"))
            .withColumn("market_cap_usd", col("market_cap_usd").cast("double"))
            .withColumn("csupply", col("csupply").cast("double"))
            .withColumn("tsupply", col("tsupply").cast("double"))
            .withColumn("msupply", col("msupply").cast("double"))
        )

        batch_df.createOrReplaceTempView("temp_coins")
        spark.sql("""
            INSERT INTO dim_coins (
                coin_id, symbol, name, nameid, rank, price_usd, percent_change_24h, percent_change_1h,
                percent_change_7d, price_btc, market_cap_usd, csupply, tsupply, msupply, created_at, updated_at
            )
            SELECT 
                coin_id, symbol, name, nameid, rank, price_usd, percent_change_24h, percent_change_1h,
                percent_change_7d, price_btc, market_cap_usd, csupply, tsupply, msupply, 
                COALESCE(created_at, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP
            FROM temp_coins
            ON CONFLICT (coin_id)
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                nameid = EXCLUDED.nameid,
                rank = EXCLUDED.rank,
                price_usd = EXCLUDED.price_usd,
                percent_change_24h = EXCLUDED.percent_change_24h,
                percent_change_1h = EXCLUDED.percent_change_1h,
                percent_change_7d = EXCLUDED.percent_change_7d,
                price_btc = EXCLUDED.price_btc,
                market_cap_usd = EXCLUDED.market_cap_usd,
                csupply = EXCLUDED.csupply,
                tsupply = EXCLUDED.tsupply,
                msupply = EXCLUDED.msupply,
                updated_at = CURRENT_TIMESTAMP
        """).write.format("jdbc").mode("append").option("url", jdbc_url).option(
            "dbtable", "dim_coins"
        ).options(**properties).save()
        logging.info(f"Batch {batch_id}: Successfully upserted into dim_coins")
    except Exception as e:
        logging.error(f"Batch {batch_id}: Failed to upsert dim_coins - {str(e)}")


def upsert_fact_coin_metrics(batch_df, batch_id):
    try:
        # Cast StringType to DoubleType for numeric fields
        batch_df = (
            batch_df.withColumn("price_usd", col("price_usd").cast("double"))
            .withColumn("percent_change_24h", col("percent_change_24h").cast("double"))
            .withColumn("percent_change_1h", col("percent_change_1h").cast("double"))
            .withColumn("percent_change_7d", col("percent_change_7d").cast("double"))
            .withColumn("market_cap_usd", col("market_cap_usd").cast("double"))
            .withColumn("csupply", col("csupply").cast("double"))
            .withColumn("api_timestamp", col("api_timestamp").cast("timestamp"))
        )

        batch_df.createOrReplaceTempView("temp_metrics")
        spark.sql("""
            INSERT INTO fact_coin_metrics (
                coin_id, date_id, price_usd, percent_change_24h, percent_change_1h, percent_change_7d,
                market_cap_usd, volume24, csupply, api_timestamp
            )
            SELECT 
                coin_id, date_id, price_usd, percent_change_24h, percent_change_1h, percent_change_7d,
                market_cap_usd, volume24, csupply, api_timestamp
            FROM temp_metrics
            ON CONFLICT (coin_id, date_id)
            DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                percent_change_24h = EXCLUDED.percent_change_24h,
                percent_change_1h = EXCLUDED.percent_change_1h,
                percent_change_7d = EXCLUDED.percent_change_7d,
                market_cap_usd = EXCLUDED.market_cap_usd,
                volume24 = EXCLUDED.volume24,
                csupply = EXCLUDED.csupply,
                api_timestamp = EXCLUDED.api_timestamp,
                ingestion_timestamp = CURRENT_TIMESTAMP
        """).write.format("jdbc").mode("append").option("url", jdbc_url).option(
            "dbtable", "fact_coin_metrics"
        ).options(**properties).save()
        logging.info(f"Batch {batch_id}: Successfully upserted into fact_coin_metrics")
    except Exception as e:
        logging.error(
            f"Batch {batch_id}: Failed to upsert fact_coin_metrics - {str(e)}"
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("streaming_load.log")],
    )

    spark = create_spark_session()

    # Read streaming Parquet data
    df = (
        spark.readStream.format("parquet")  # Fixed typo from "parque"
        .schema(coin_schema)
        .option("path", f"s3a://{MINIO_BUCKET_RAW_PRICE}/raw-data/")
        .option("maxFilesPerTrigger", 10)
        .load()
        .withColumn(
            "date_id",
            expr("date_format(to_timestamp(api_timestamp / 1000), 'yyyyMMdd')").cast(
                "integer"
            ),
        )
        .withWatermark("api_timestamp", "10 minutes")
        .dropDuplicates(["id", "date_id"])
        .withColumnRenamed("id", "coin_id")
        .withColumn("created_at", col("ingestion_timestamp").cast("timestamp"))
    )

    # Upsert to dim_coins
    coins_query = (
        df.writeStream.foreachBatch(upsert_dim_coins)
        .outputMode("append")
        .trigger(processingTime="10 seconds")  # Adjusted for faster processing
        .start()
    )

    # Upsert to fact_coin_metrics
    metrics_query = (
        df.writeStream.foreachBatch(upsert_fact_coin_metrics)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Keep the script running
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logging.info("Shutting down streaming application")
        spark.stop()
