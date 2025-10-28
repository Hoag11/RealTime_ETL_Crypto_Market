"""Compatibility layer for legacy imports.

The legacy ``configs`` module previously exposed a collection of constants. The
new configuration system lives in :mod:`core.config` and exposes a ``Settings``
dataclass via :func:`core.config.get_settings`.  This module now simply loads
those settings once and mirrors the original attribute names so that existing
imports keep working.
"""

from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core.config import get_settings

_settings = get_settings()

# KAFKA CONFIG
KAKFA_BROKER = _settings.kafka_brokers
KAFKA_TOPIC = _settings.kafka_topic
KAFKA_SECURITY_PROCTOCOL = _settings.kafka_security_protocol
KAFKA_SASL_MECHANISM = _settings.kafka_sasl_mechanism
KAFKA_SASL_USERNAME = _settings.kafka_sasl_username
KAFKA_SASL_PASSWORD = _settings.kafka_sasl_password

# API CONFIG
API_STREAMING = _settings.api_streaming_url
API_BATCHING = _settings.api_batching_url
API_HISTORICAL = _settings.api_historical_url

# MinIO CONFIG
MINIO_ENDPOINT = _settings.minio_endpoint
MINIO_ACCESS_KEY = _settings.minio_access_key
MINIO_SECRET_KEY = _settings.minio_secret_key
MINIO_BUCKET_RAW_PRICE = _settings.minio_bucket_raw_price
MINIO_BUCKET_RAW_NEWS = _settings.minio_bucket_raw_news

# POSTGRES CONFIG
PG_HOST = _settings.postgres_host
PG_PORT = _settings.postgres_port
PG_USER = _settings.postgres_user
PG_PASSWORD = _settings.postgres_password
PG_DB = _settings.postgres_db

# SPARK CONFIG
SPARK_MASTER = _settings.spark_master
SPARK_APP_NAME = _settings.spark_app_name

COINS = _settings.coins

__all__ = [
    "API_BATCHING",
    "API_HISTORICAL",
    "API_STREAMING",
    "COINS",
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
