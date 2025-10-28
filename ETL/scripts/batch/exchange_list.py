import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

sys.path.insert(0, parent_dir)

import io
import logging
import json
from datetime import datetime
from configs import configs
from minio import Minio
from minio.error import S3Error
from requests import RequestException

from scripts.common import http_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

API = getattr(configs, "API_HISTORICAL", "https://api.coingecko.com/api/v3")
BUCKET_NAME = getattr(configs, "MINIO_BUCKET_RAW_PRICE", None)
MINIO_ENDPOINT = configs.MINIO_ENDPOINT.replace("http://", "")
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
MINIO_SECURE = False

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
        response = http_client.get(url)

        logging.info(
            "Data fetched successfully from %s (status %s, %.2fs)",
            url,
            response.status_code,
            response.elapsed.total_seconds(),
        )

        data = response.json()
        if not data:
            logging.warning("No data found in the response")
    except RequestException as e:
        logging.error("Request error: %s", e)
        return None
    return data


def get_data(**context):
    ti = context.get("ti")
    if not ti:
        logging.error("No task instance found in context")
        return None

    exchange_data = ti.xcom_pull(task_ids="fetch_exchange_data")
    if not exchange_data:
        logging.error("No data retrieved from XCom")
        return None

    try:
        if not isinstance(exchange_data, list):
            logging.warning("Exchange data is not a list, got %s", type(exchange_data))
            return None
    except Exception as e:
        logging.error("Error parsing data: %s", e)
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

        logging.info("Successfully uploaded data to %s/%s", BUCKET_NAME, object_name)
        return object_name

    except S3Error as e:
        logging.error("MinIO error: %s", e)
        return None
