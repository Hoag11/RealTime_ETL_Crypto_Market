"""Application configuration helpers.

This module centralizes configuration loading so that scripts and Spark jobs
share the same source of truth. Values are read from environment variables and
an optional `.env` file at the project root. Safe defaults mirroring the
previous hard-coded constants are provided to make local development seamless.
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, List

# Ensure a predictable logging configuration the first time settings are loaded.
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def _load_env_file() -> None:
    """Load key/value pairs from a `.env` file if it exists.

    Values that already exist in ``os.environ`` are preserved to honour the
    caller's explicit configuration.
    """

    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def _get_list(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name)
    if raw:
        return [item.strip() for item in raw.split(",") if item.strip()]
    return list(default)


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logging.getLogger(__name__).warning(
            "Invalid value '%s' for %s. Falling back to %s.", raw, name, default
        )
        return default


DEFAULT_COINS: List[Dict[str, str]] = [
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


@dataclass(frozen=True)
class Settings:
    kafka_brokers: List[str] = field(default_factory=list)
    kafka_topic: str = ""
    kafka_security_protocol: str = ""
    kafka_sasl_mechanism: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""

    api_streaming_url: str = ""
    api_batching_url: str = ""
    api_historical_url: str = ""

    minio_endpoint: str = ""
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket_raw_price: str = ""
    minio_bucket_raw_news: str = ""

    postgres_host: str = ""
    postgres_port: int = 0
    postgres_user: str = ""
    postgres_password: str = ""
    postgres_db: str = ""

    spark_master: str = ""
    spark_app_name: str = ""

    coins: List[Dict[str, str]] = field(default_factory=list)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached ``Settings`` instance populated from the environment."""

    _load_env_file()
    return Settings(
        kafka_brokers=_get_list(
            "KAFKA_BROKERS", ["kafka-0:9092", "kafka-1:9092", "kafka-2:9092"]
        ),
        kafka_topic=os.getenv("KAFKA_TOPIC", "coin_prices"),
        kafka_security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        kafka_sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
        kafka_sasl_username=os.getenv("KAFKA_SASL_USERNAME", "admin"),
        kafka_sasl_password=os.getenv("KAFKA_SASL_PASSWORD", "Unigap@2024"),
        api_streaming_url=os.getenv(
            "API_STREAMING_URL", "https://api.coinlore.net/api/tickers/"
        ),
        api_batching_url=os.getenv(
            "API_BATCHING_URL",
            "https://data-api.coindesk.com/news/v1/article/list?lang=EN&limit=100",
        ),
        api_historical_url=os.getenv(
            "API_HISTORICAL_URL", "https://api.coingecko.com/api/v3"
        ),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "admin123"),
        minio_bucket_raw_price=os.getenv("MINIO_BUCKET_RAW_PRICE", "crypto-data-raw"),
        minio_bucket_raw_news=os.getenv("MINIO_BUCKET_RAW_NEWS", "crypto-news-raw"),
        postgres_host=os.getenv("POSTGRES_HOST", "postgres-bi"),
        postgres_port=_get_int("POSTGRES_PORT", 5432),
        postgres_user=os.getenv("POSTGRES_USER", "postgres"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        postgres_db=os.getenv("POSTGRES_DB", "warehouse"),
        spark_master=os.getenv("SPARK_MASTER", "spark-spark-1:7077"),
        spark_app_name=os.getenv("SPARK_APP_NAME", "streamingCryptoApp"),
        coins=DEFAULT_COINS,
    )


__all__ = ["Settings", "get_settings"]
