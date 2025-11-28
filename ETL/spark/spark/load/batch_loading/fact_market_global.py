import sys, traceback, logging
from pyspark.sql import SparkSession, functions as F
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s"
)
log = logging.getLogger("fact_market_global")

# --- CONFIG --- #
if "/configs" not in sys.path:
    sys.path.insert(0, "/configs")
import configs

BUCKET_NAME = getattr(configs, "MINIO_BUCKET_RAW_PRICE", "crypto-data-raw")
MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY = (
    configs.MINIO_ENDPOINT,
    configs.MINIO_ACCESS_KEY,
    configs.MINIO_SECRET_KEY,
)
PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB = (
    configs.PG_HOST,
    configs.PG_PORT,
    configs.PG_USER,
    configs.PG_PASSWORD,
    configs.PG_DB,
)
PATH_OBJECT = f"s3a://{BUCKET_NAME}/global_market/*.json"


def upsert_postgres(df, table_name, batch_size=500):
    """UPSERT theo batch 500 dòng"""
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    rows = [r.asDict() for r in df.collect()]
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        with engine.begin() as conn:
            for row in batch:
                row_dict = {
                    k: (v.to_pydatetime() if hasattr(v, "to_pydatetime") else v)
                    for k, v in row.items()
                }
                query = text("""
                    INSERT INTO fact_market_global (
                        date_id, active_cryptocurrencies, upcoming_icos, ongoing_icos,
                        ended_icos, markets, market_cap_change_percentage_24h_usd,
                        updated_at, ingestion_timestamp
                    )
                    VALUES (
                        :date_id, :active_cryptocurrencies, :upcoming_icos, :ongoing_icos,
                        :ended_icos, :markets, :market_cap_change_percentage_24h_usd,
                        :updated_at, :ingestion_timestamp
                    )
                    ON CONFLICT (date_id)
                    DO UPDATE SET
                        active_cryptocurrencies = EXCLUDED.active_cryptocurrencies,
                        upcoming_icos = EXCLUDED.upcoming_icos,
                        ongoing_icos = EXCLUDED.ongoing_icos,
                        ended_icos = EXCLUDED.ended_icos,
                        markets = EXCLUDED.markets,
                        market_cap_change_percentage_24h_usd = EXCLUDED.market_cap_change_percentage_24h_usd,
                        updated_at = EXCLUDED.updated_at,
                        ingestion_timestamp = EXCLUDED.ingestion_timestamp;
                """)
                conn.execute(query, row_dict)


def main():
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("Load fact_market_global_upsert")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )

        log.info(f"Đọc dữ liệu từ {PATH_OBJECT}")
        df = spark.read.option("multiline", "true").json(PATH_OBJECT)
        if df.rdd.isEmpty():
            log.warning("Không có dữ liệu JSON trong bucket.")
            return

        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        pg_props = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }
        dim_dates = spark.read.jdbc(jdbc_url, "dim_dates", properties=pg_props).select(
            "date_id", F.to_date("full_date").alias("full_date")
        )

        df_final = (
            df.select(
                F.col("active_cryptocurrencies").cast("int"),
                F.col("upcoming_icos").cast("int"),
                F.col("ongoing_icos").cast("int"),
                F.col("ended_icos").cast("int"),
                F.col("markets").cast("int"),
                F.col("market_cap_change_percentage_24h_usd").cast("double"),
                F.col("updated_at").cast("timestamp"),
            )
            .withColumn("date_only", F.to_date("updated_at"))
            .join(dim_dates, F.col("date_only") == F.col("full_date"), "left")
            .select(
                "date_id",
                "active_cryptocurrencies",
                "upcoming_icos",
                "ongoing_icos",
                "ended_icos",
                "markets",
                "market_cap_change_percentage_24h_usd",
                "updated_at",
                F.current_timestamp().alias("ingestion_timestamp"),
            )
        )

        upsert_postgres(df_final, "fact_market_global")
        log.info("Hoàn thành job fact_market_global")

    except Exception as e:
        log.error(f"Lỗi job fact_market_global: {e}")
        traceback.print_exc()
    finally:
        if spark:
            spark.stop()
            log.info("Đóng SparkSession")


if __name__ == "__main__":
    main()
