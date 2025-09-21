import logging
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logging")

os.makedirs(LOG_DIR, exist_ok=True)

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
KAFKA_SASL_PASSWORD = "Unigap@2024"

# API CONFIG
API_STREAMING = "https://api.coinlore.net/api/tickers/"
API_BATCHING = "https://data-api.coindesk.com/news/v1/article/list?lang=EN&limit=100"
API_HISTORICAL = "https://api.coingecko.com/api/v3"

# MinIO CONFIG
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET_RAW_PRICE = "crypto-data-raw"
MINIO_BUCKET_PROCESSED_PRICE = "crypto-data-processed"
MINIO_BUCKET_RAW_NEWS = "crypto-news-raw"
MINIO_BUCKET_PROCESSED_NEWS = "crypto-news-processed"
MINIO_BUCKET_HISTORICAL = "crypto-historical-data"

# POSTGRES CONFIG
PG_HOST = "postgres-bi"
PG_PORT = 5432
PG_DB_PRICE = "crypto_price"
PG_DB_NEWS = "crypto_news"
PG_USER = "postgres"
PG_PASSWORD = "postgres"

# SPARK CONFIG
SPARK_MASTER = "spark-spark-1:7077"
SPARK_APP_NAME = "streamingCryptoApp"


COINS = [
    {"id": "osmosis-allbtc", "name": "Bitcoin"},
    {"id": "the-ticker-is-eth", "name": "Ethereum"},
    {"id": "warioxrpdumbledoreyugioh69inu", "name": "XRP"},
    {"id": "zircuit-bridged-usdt-zircuit", "name": "Tether"},
    {"id": "binance-coin-wormhole", "name": "Binance Coin"},
    {"id": "wrapped-solana", "name": "Solana"},
    {"id": "zksync-bridged-usdc-zksync", "name": "USD Coin"},
    {"id": "staked-ether", "name": "Staked Ether"},
    {"id": "doge-on-pulsechain", "name": "Dogecoin"},
    {"id": "tron-bsc", "name": "TRON"},
    {"id": "cardano", "name": "Cardano"},
    {"id": "wormhole-bridged-hype", "name": "Hyperliquid"},
    {"id": "osmosis-alllink", "name": "ChainLink"},
    {"id": "zora-bridged-weth-zora-network", "name": "WETH"},
    {"id": "zksync-bridged-wbtc-zksync", "name": "Wrapped Bitcoin"},
    {"id": "wrapped-beacon-eth", "name": "Wrapped Beacon ETH"},
    {"id": "okb", "name": "OKB"},
    {"id": "bitcoin-cash", "name": "Bitcoin Cash"},
    {"id": "sui", "name": "Sui"},
    {"id": "stellar", "name": "Stellar"},
    {"id": "binance-peg-avalanche", "name": "Avalanche"},
    {"id": "hedera-hashgraph", "name": "Hedera Hashgraph"},
    {"id": "litecoin", "name": "Litecoin"},
    {"id": "tontoken", "name": "TON Coin"},
    {"id": "strategic-hub-for-innovation-in-blockchain", "name": "Shiba Inu"},
    {"id": "xbanking-usde", "name": "Ethena USDe"},
    {"id": "xcdot", "name": "Polkadot"},
    {"id": "uniswap-wormhole", "name": "Uniswap"},
    {"id": "bitget-token", "name": "Bitget Token"},
    {"id": "zksync-erc20-bridged-dai-zksync", "name": "Dai"},
    {"id": "monero", "name": "Monero"},
    {"id": "ethena", "name": "Ethena"},
    {"id": "aave", "name": "Aave"},
    {"id": "pepe-token", "name": "Pepe"},
    {"id": "mynth", "name": "Mantle"},
    {"id": "ethereum-classic", "name": "Ethereum Classic"},
    {"id": "the-anthropic-order", "name": "Bittensor"},
    {"id": "near", "name": "NEAR Protocol"},
    {"id": "ondo-finance", "name": "Ondo"},
    {"id": "aptos", "name": "Aptos"},
    {"id": "pi-network-iou", "name": "Pi"},
    {"id": "internet-computer", "name": "Internet Computer (IOU)"},
    {"id": "osmosis-allarb", "name": "Arbitrum"},
    {"id": "RENDER", "name": "Render Token"},
    {"id": "crypto-com-chain", "name": "Crypto.com Chain"},
    {"id": "story-2", "name": "Story"},
    {"id": "wrapped-kaspa", "name": "Kaspa"},
    {"id": "binance-staked-sol", "name": "Binance Staked SOL"},
    {"id": "vechain", "name": "VeChain"},
    {"id": "algorand", "name": "Algorand"},
    {"id": "worldcoin-wld", "name": "Worldcoin"},
    {"id": "pudgy-penguins", "name": "Pudgy Penguins"},
    {"id": "render-token", "name": "Render"},
    {"id": "lost-bitcoin-layer", "name": "Cosmos"},
    {"id": "sei-network", "name": "Sei"},
    {"id": "trumpeffect69420", "name": "OFFICIAL TRUMP"},
    {"id": "bonk-on-eth", "name": "Bonk"},
    {"id": "filecoin", "name": "Filecoin"},
    {"id": "maker", "name": "Maker"},
    {"id": "flare-networks", "name": "Flare"},
    {"id": "fetch-ai", "name": "Fetch.ai"},
    {"id": "first-digital-usd", "name": "First Digital USD"},
    {"id": "injective-protocol", "name": "Injective Protocol"},
    {"id": "kucoin-shares", "name": "KuCoin Shares"},
    {"id": "optimism", "name": "Optimism"},
    {"id": "quant-network", "name": "Quant"},
    {"id": "tia", "name": "Celestia"},
    {"id": "spx6900", "name": "SPX6900"},
    {"id": "curve-dao-token", "name": "Curve DAO Token"},
    {"id": "linea-bridged-ldo-linea", "name": "Lido DAO"},
    {"id": "pax-gold", "name": "PAX Gold"},
    {"id": "stox", "name": "Stacks"},
    {"id": "aerodrome-finance", "name": "Aerodrome Finance"},
    {"id": "immutable-x", "name": "Immutable"},
    {"id": "XDCE", "name": "XinFin Network"},
    {"id": "pyth-network", "name": "Pyth Network"},
    {"id": "kaia", "name": "Kaia"},
    {"id": "tether-gold", "name": "Tether Gold"},
    {"id": "fantom", "name": "Fantom"},
    {"id": "the-graph", "name": "The Graph"},
    {"id": "cosmic-force-token-v2", "name": "Conflux"},
    {"id": "token-s", "name": "Sonic (prev. FTM)"},
    {"id": "flokiwifhat", "name": "Floki Inu"},
    {"id": "raydium", "name": "Raydium"},
    {"id": "paypal-usd", "name": "PayPal USD"},
    {"id": "pancakeswap-token", "name": "PancakeSwap"},
    {"id": "wif-secondchance", "name": "dogwifhat"},
    {"id": "nexo", "name": "Nexo"},
    {"id": "ethereum-name-service", "name": "Ethereum Name Service"},
    {"id": "matic-network", "name": "Matic Network"},
    {"id": "theta-token", "name": "Theta Token"},
    {"id": "singularitynet", "name": "SingularityNET"},
    {"id": "pendle", "name": "Pendle"},
    {"id": "zcash", "name": "Zcash"},
    {"id": "virtual-protocol", "name": "Virtual Protocol"},
    {"id": "tezos", "name": "Tezos"},
    {"id": "the-sandbox-wormhole", "name": "The Sandbox"},
    {"id": "vaulta", "name": "Vaulta"},
    {"id": "fartcoin", "name": "Fartcoin"},
    {"id": "gala", "name": "Gala"},
]
