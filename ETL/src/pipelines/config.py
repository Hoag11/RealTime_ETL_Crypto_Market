"""Runtime configuration helpers for the crypto ETL pipelines.

This module exposes the same attribute-based configuration interface that the
legacy ``configs`` package provided, but values can now be overridden via
environment variables or (when running inside Airflow) Variables. The
convention is to look up both the plain name (e.g. ``MINIO_ENDPOINT``) and the
``CRYPTO_ETL_`` prefixed variant before falling back to the default.
"""
from __future__ import annotations

import logging
import os
from typing import Iterable, List

try:  # pragma: no cover - Airflow is not available in the test environment
    from airflow.models import Variable  # type: ignore
except Exception:  # pragma: no cover - graceful fallback outside Airflow
    Variable = None  # type: ignore


LOGGER = logging.getLogger(__name__)


def _resolve_runtime_value(name: str, default: str | None = None) -> str | None:
    """Return a configuration value from env vars or Airflow Variables."""
    candidates: Iterable[str] = (name, f"CRYPTO_ETL_{name}")
    for candidate in candidates:
        env_value = os.environ.get(candidate)
        if env_value is not None:
            return env_value
    if Variable is not None:
        for candidate in candidates:
            try:
                return Variable.get(candidate)
            except KeyError:
                continue
            except Exception as exc:  # pragma: no cover - defensive fallback
                LOGGER.debug("Unable to read Airflow Variable %s: %s", candidate, exc)
                continue
    return default


def _resolve_int(name: str, default: int) -> int:
    value = _resolve_runtime_value(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        LOGGER.warning(
            "Invalid integer for %s=%s, falling back to default %s", name, value, default
        )
        return default


def _resolve_list(name: str, default: List[str]) -> List[str]:
    value = _resolve_runtime_value(name)
    if not value:
        return list(default)
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or list(default)


# Kafka configuration -----------------------------------------------------
KAKFA_BROKER = _resolve_list(
    "KAFKA_BROKER", ["kafka-0:9092", "kafka-1:9092", "kafka-2:9092"]
)
KAFKA_BROKER = KAKFA_BROKER  # backwards compatible alias
KAFKA_TOPIC = _resolve_runtime_value("KAFKA_TOPIC", "coin_prices")
KAFKA_SECURITY_PROCTOCOL = _resolve_runtime_value("KAFKA_SECURITY_PROCTOCOL", "SASL_PLAINTEXT")
KAFKA_SASL_MECHANISM = _resolve_runtime_value("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = _resolve_runtime_value("KAFKA_SASL_USERNAME", "admin")
KAFKA_SASL_PASSWORD = _resolve_runtime_value("KAFKA_SASL_PASSWORD", "Unigap@2024")

# API configuration -------------------------------------------------------
API_STREAMING = _resolve_runtime_value(
    "API_STREAMING", "https://api.coinlore.net/api/tickers/"
)
API_BATCHING = _resolve_runtime_value(
    "API_BATCHING",
    "https://data-api.coindesk.com/news/v1/article/list?lang=EN&limit=100",
)
API_HISTORICAL = _resolve_runtime_value(
    "API_HISTORICAL", "https://api.coingecko.com/api/v3"
)

# MinIO configuration -----------------------------------------------------
MINIO_ENDPOINT = _resolve_runtime_value("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = _resolve_runtime_value("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = _resolve_runtime_value("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET_RAW_PRICE = _resolve_runtime_value("MINIO_BUCKET_RAW_PRICE", "crypto-data-raw")
MINIO_BUCKET_RAW_NEWS = _resolve_runtime_value("MINIO_BUCKET_RAW_NEWS", "crypto-news-raw")

# PostgreSQL configuration -----------------------------------------------
PG_HOST = _resolve_runtime_value("PG_HOST", "postgres-bi")
PG_PORT = _resolve_int("PG_PORT", 5432)
PG_USER = _resolve_runtime_value("PG_USER", "postgres")
PG_PASSWORD = _resolve_runtime_value("PG_PASSWORD", "postgres")
PG_DB = _resolve_runtime_value("PG_DB", "warehouse")

# Spark configuration -----------------------------------------------------
SPARK_MASTER = _resolve_runtime_value("SPARK_MASTER", "spark-spark-1:7077")
SPARK_APP_NAME = _resolve_runtime_value("SPARK_APP_NAME", "streamingCryptoApp")

# Static coin list --------------------------------------------------------
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

__all__ = [
    "API_BATCHING",
    "API_HISTORICAL",
    "API_STREAMING",
    "COINS",
    "KAFKA_BROKER",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SECURITY_PROCTOCOL",
    "KAKFA_BROKER",
    "KAFKA_TOPIC",
    "MINIO_ACCESS_KEY",
    "MINIO_BUCKET_RAW_NEWS",
    "MINIO_BUCKET_RAW_PRICE",
    "MINIO_ENDPOINT",
    "MINIO_SECRET_KEY",
    "PG_DB",
    "PG_HOST",
    "PG_PASSWORD",
    "PG_PORT",
    "PG_USER",
    "SPARK_APP_NAME",
    "SPARK_MASTER",
]
