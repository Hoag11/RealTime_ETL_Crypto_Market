import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

sys.path.insert(0, parent_dir)

import requests
import io
import logging
import json
from fake_useragent import UserAgent
from datetime import datetime
from configs import configs
from minio import Minio
from minio.error import S3Error

ua = UserAgent()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

API = getattr(configs, "API_HISTORICAL", "https://api.coingecko.com/api/v3")
BUCKET_NAME = getattr(configs, "MINIO_BUCKET_RAW_PRICE", None)
MINIO_ENDPOINT = configs.MINIO_ENDPOINT.replace("http://", "")
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
MINIO_SECURE = False

OBJECT_FOLDER = "global_market"

RATE_LIMIT_SECONDS = 15
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def _init_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def fetch_data(**context):
    url = f"{API}/global"

    try:
        response = requests.get(url, headers={"User-Agent": ua.random})
        response.raise_for_status()  # Raise an error for bad status codes

        logging.info("Data fetched successfully from %s", url)
        logging.info("Response status code: %s", response.status_code)

        data = response.json()

        if not data or "data" not in data:
            logging.warning("No data found in the response")
            return None
    except requests.RequestException as e:
        logging.error("Request error: %s", e)
        return None
    return json.dumps(data)


def get_data(**context):
    ti = context.get("ti")
    if not ti:
        logging.error("No task instance found in context")
        return None

    global_data = ti.xcom_pull(task_ids="fetch_global_market_data")
    if not global_data:
        logging.error("No data retrieved from XCom")
        return None

    try:
        parsed = json.loads(global_data)  # parse string -> dict
        parsed_data = parsed.get("data", {})
        if not parsed_data:
            logging.warning("Parsed data is empty")
            return None
    except Exception as e:
        logging.error("Error parsing data: %s", e)
        return None

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    object_name = f"{OBJECT_FOLDER}/global_market_{today_str}.json"

    data_bytes = json.dumps(parsed_data, ensure_ascii=False, indent=2).encode("utf-8")

    try:
        # init client
        client = _init_minio_client()

        # ensure bucket exists
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)

        # upload object
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
