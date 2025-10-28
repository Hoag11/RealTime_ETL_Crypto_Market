import io
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import requests
from fake_useragent import UserAgent
from minio import Minio
from minio.error import S3Error

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core.config import get_settings

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

settings = get_settings()
ua = UserAgent()
API = settings.api_historical_url
BUCKET_NAME = settings.minio_bucket_raw_price
_endpoint = settings.minio_endpoint
MINIO_ENDPOINT = _endpoint.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = settings.minio_access_key
MINIO_SECRET_KEY = settings.minio_secret_key
MINIO_SECURE = _endpoint.startswith("https://")

OBJECT_FOLDER = "exchange_list"
RATE_LIMIT_SECONDS = 15


def _init_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def fetch_data(**context):
    url = f"{API}/exchanges/list"

    try:
        response = requests.get(url, headers={"User-Agent": ua.random})
        response.raise_for_status()

        logger.info("Data fetched successfully from %s", url)
        logger.info("Response status code: %s", response.status_code)

        data = response.json()
        if not data:
            logger.warning("No data found in the response")
    except requests.RequestException as e:
        logger.error("Request error: %s", e)
        return None
    return data


def get_data(**context):
    ti = context.get("ti")
    if not ti:
        logger.error("No task instance found in context")
        return None

    exchange_data = ti.xcom_pull(task_ids="fetch_exchange_data")
    if not exchange_data:
        logger.error("No data retrieved from XCom")
        return None

    if not isinstance(exchange_data, list):
        logger.warning("Exchange data is not a list, got %s", type(exchange_data))
        return None

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    object_name = f"{OBJECT_FOLDER}/exchanges_list_{today_str}.json"

    data_bytes = json.dumps(exchange_data, ensure_ascii=False, indent=2).encode("utf-8")

    try:
        client = _init_minio_client()
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)

        client.put_object(
            BUCKET_NAME,
            object_name,
            io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="application/json",
        )

        logger.info("Successfully uploaded data to %s/%s", BUCKET_NAME, object_name)
        return object_name

    except S3Error as e:
        logger.error("MinIO error: %s", e)
        return None
