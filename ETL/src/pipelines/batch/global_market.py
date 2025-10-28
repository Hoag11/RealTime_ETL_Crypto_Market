"""Global market batch ingestion helpers."""
from __future__ import annotations

import io
import json
import logging
from datetime import datetime
from typing import Optional

import requests
from fake_useragent import UserAgent
from minio import Minio
from minio.error import S3Error

from pipelines import config

ua = UserAgent()
API = config.API_HISTORICAL
BUCKET_NAME = config.MINIO_BUCKET_RAW_PRICE
MINIO_ENDPOINT = config.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = config.MINIO_SECRET_KEY
MINIO_SECURE = config.MINIO_ENDPOINT.startswith("https")
OBJECT_FOLDER = "global_market"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _init_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def fetch_data(**_context) -> Optional[str]:
    url = f"{API}/global"
    try:
        response = requests.get(url, headers={"User-Agent": ua.random}, timeout=30)
        response.raise_for_status()
        logging.info("Data fetched successfully from %s (status %s)", url, response.status_code)
        data = response.json()
        if not data or "data" not in data:
            logging.warning("No data found in the response")
            return None
        return json.dumps(data)
    except requests.RequestException as exc:
        logging.error("Request error while fetching global data: %s", exc)
        return None


def get_data(**context) -> Optional[str]:
    ti = context.get("ti")
    if not ti:
        logging.error("No task instance found in context")
        return None

    global_data = ti.xcom_pull(task_ids="fetch_global_market_data")
    if not global_data:
        logging.error("No data retrieved from XCom")
        return None

    try:
        parsed = json.loads(global_data)
        parsed_data = parsed.get("data", {}) if isinstance(parsed, dict) else {}
        if not parsed_data:
            logging.warning("Parsed global market data is empty")
            return None
    except Exception as exc:
        logging.error("Error parsing data: %s", exc)
        return None

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    object_name = f"{OBJECT_FOLDER}/global_market_{today_str}.json"
    data_bytes = json.dumps(parsed_data, ensure_ascii=False, indent=2).encode("utf-8")

    client = _init_minio_client()
    try:
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
    except S3Error as exc:
        logging.error("MinIO error: %s", exc)
        return None
