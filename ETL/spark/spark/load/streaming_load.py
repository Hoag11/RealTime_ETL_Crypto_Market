import logging
from pyspark.sql import SparkSession, functions as F, types as T
import configs
import psycopg2
from psycopg2 import sql
from pyspark.sql.window import Window


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BUCKET = configs.MINIO_BUCKET_RAW_PRICE.rstrip("/")
RAW_PATH = f"s3a://{BUCKET}/raw-data/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/_checkpoints/fact_coin_metrics_stream/"
POSTGRES_URL = f"jdbc:postgresql://{configs.PG_HOST}:{configs.PG_PORT}/{configs.PG_DB}"
POSTGRES_PROPS = {
    "user": configs.PG_USER,
    "password": configs.PG_PASSWORD,
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
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            configs.MINIO_ENDPOINT.replace("http://", ""),
        )
        .config("spark.hadoop.fs.s3a.access.key", configs.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", configs.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_batch(df, batch_id):
    if df.isEmpty():
        logging.info(f"Batch {batch_id}: No data to process.")
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
        F.col("id").alias("coin_id"),
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
    window_spec = Window.partitionBy("coin_id", "date_id").orderBy(
        F.col("api_timestamp").desc()
    )
    df_dedup = (
        df_final.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    record_count = df_dedup.count()
    if record_count == 0:
        logging.info(f"Batch {batch_id}: No valid records after deduplication.")
        return

    logging.info(
        f"Batch {batch_id}: Writing {record_count} deduplicated rows to stage table..."
    )

    # 6️⃣ Ghi vào bảng stage
    STAGE_TABLE = "fact_coin_metrics_stage"
    df_dedup.write.jdbc(POSTGRES_URL, STAGE_TABLE, "overwrite", POSTGRES_PROPS)

    # 7️⃣ Merge dữ liệu từ stage → fact_coin_metrics
    merge_query = """
        INSERT INTO fact_coin_metrics AS fcm
        SELECT * FROM fact_coin_metrics_stage
        ON CONFLICT (coin_id, date_id)
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
            host=configs.PG_HOST,
            database=configs.PG_DB,
            user=configs.PG_USER,
            password=configs.PG_PASSWORD,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(merge_query)
        conn.close()
        logging.info(f"✅ Batch {batch_id}: Upsert completed successfully.")
    except Exception as e:
        logging.error(f"❌ Upsert failed for batch {batch_id}: {e}")
    finally:
        # 9️⃣ Dọn bảng stage sau mỗi batch
        try:
            conn = psycopg2.connect(
                host=configs.PG_HOST,
                database=configs.PG_DB,
                user=configs.PG_USER,
                password=configs.PG_PASSWORD,
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE fact_coin_metrics_stage;"))
            conn.close()
            logging.info("🧹 Stage table truncated after merge.")
        except Exception as e:
            logging.warning(f"⚠️ Could not truncate stage table: {e}")


def main():
    spark = start_spark()

    logging.info(f"🔄 Reading streaming data from {RAW_PATH} ...")
    df_stream = spark.readStream.schema(parquet_schema).parquet(f"{RAW_PATH}date=*")

    query = (
        df_stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="1 minute")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Spark streaming job failed: {e}")
    finally:
        logging.info("Closing SparkSession.")
