import logging
import os
from datetime import datetime

# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# LOG_DIR = os.path.join(BASE_DIR, "logging")
#
# os.makedirs(LOG_DIR, exist_ok=True)
#
timestamp = datetime.now().strftime("%Y%m%d")
# LOG_FILE = os.path.join(LOG_DIR, f"{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# KAFKA CONFIG
KAKFA_BROKER = ["kafka-0:9092", "kafka-1:9092", "kafka-2:9092"]

KAFKA_TOPIC = "coin_prices"

KAFKA_SECURITY_PROCTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_USERNAME = "admin"
KAFKA_SASL_PASSWORD = "admin"

# API CONFIG
API_STREAMING = "https://api.coinlore.net/api/tickers/"
API_BATCHING = "https://data-api.coindesk.com/news/v1/article/list?lang=EN&limit=100"
API_HISTORICAL = "https://api.coingecko.com/api/v3"

# MinIO CONFIG
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET_RAW_PRICE = "crypto-data-raw"
MINIO_BUCKET_RAW_NEWS = "crypto-news-raw"

# POSTGRES CONFIG
PG_HOST = "postgres-bi"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DB = "warehouse"

# SPARK CONFIG
SPARK_MASTER = "spark-spark-1:7077"
SPARK_APP_NAME = "streamingCryptoApp"


COINS = [
    "BTC", "ETH", "XRP", "USDT", "BNB", "SOL", "USDC", "STETH", "DOGE", "TRX",
    "ADA", "HYPE", "LINK", "WETH", "WBTC", "WBETH", "OKB", "BCH", "SUI", "XLM",
    "AVAX", "HBAR", "LTC", "TON", "SHIB", "USDE", "DOT", "UNI", "BGB", "DAI",
    "XMR", "ENA", "AAVE", "PEPE", "MNT", "ETC", "TAO", "NEAR", "ONDO", "APT",
    "PI", "ICP", "ARB", "RNDR", "CRO", "IP", "KAS", "BNSOL", "VET", "ALGO",
    "WLD", "PENGU", "RENDER", "ATOM", "SEI", "TRUMP", "BONK", "FIL", "MKR", "FLR",
    "FET", "FDUSD", "INJ", "KCS", "OP", "QNT", "TIA", "SPX", "CRV", "LDO",
    "PAXG", "STX", "AERO", "IMX", "XDCE", "PYTH", "KAIA", "XAUT", "FTM", "GRT",
    "CFX", "FLOKI", "RAY", "PYUSD", "CAKE", "WIF", "NEXO", "ENS", "MATIC",
    "THETA", "AGIX", "PENDLE", "ZEC", "VIRTUAL", "XTZ", "SAND", "FARTCOIN",
    "GALA"
]

