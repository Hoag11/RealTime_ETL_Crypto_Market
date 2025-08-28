from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from configs import configs
import json
import logging

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


def create_spark_session():
    spark = (
        SparkSession.builder.appName("RawIngestion")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.streaming.checkpointLocation", "spark-checkpoint-raw")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    spark = create_spark_session()

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(KAFKA_BROKER))
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option(
            "kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{
                KAFKA_SASL_USERNAME
            }' password='{KAFKA_SASL_PASSWORD}';",
        )
        .load()
    )

    raw_df = df.selectExpr(
        "CAST(value AS STRING) as value", "CAST(timestamp AS TIMESTAMP) as event_time"
    ).withColumn("date", to_date(col("event_time")))

    query = (
        raw_df.writeStream.format("parquet")
        .option("path", f"s3a://{MINIO_BUCKET_RAW_NEWS}/raw-news-data")
        .partitionBy("date")
        .start()
    )

    logging.info("Streaming to MinIO started...")

    query.awaitTermination()
