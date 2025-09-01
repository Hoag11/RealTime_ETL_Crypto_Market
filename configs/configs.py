import logging
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logging")

os.makedirs(LOG_DIR, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d")
LOG_FILE = os.path.join(LOG_DIR, f"{timestamp}.log")

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)

# KAFKA CONFIG
KAKFA_BROKER = ["localhost:9094", "localhost:9194", "localhost:9294"]

KAFKA_TOPIC = "coin_prices"

KAFKA_SECURITY_PROCTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_USERNAME = "admin"
KAFKA_SASL_PASSWORD = "Unigap@2024"

# API CONFIG
API_STREAMING = "https://api.coinlore.net/api/tickers/"

API_BATCHING = "https://data-api.coindesk.com/news/v1/article/list?lang=EN&limit=100"

# MinIO CONFIG
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET_RAW_PRICE = "crypto-data-raw"
MINIO_BUCKET_PROCESSED_PRICE = "crypto-data-processed"
MINIO_BUCKET_RAW_NEWS = "crypto-news-raw"
MINIO_BUCKET_PROCESSED_NEWS = "crypto-news-processed"

# POSTGRES CONFIG
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB_PRICE = "crypto_price"
PG_DB_NEWS = "crypto_news"
PG_USER = "admin@admin.com"
PG_PASSWORD = "admin"

# SPARK CONFIG
SPARK_MASTER = "localhost:7077"
SPARK_APP_NAME = "streamingCryptoApp"
