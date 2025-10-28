import io
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

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


def extract_news():
    """Fetches news articles from a remote API."""
    try:
        response = requests.get(
            settings.api_batching_url, headers={"User-Agent": ua.random}, timeout=10
        )
        response.raise_for_status()
        logger.info("Response status code: %s", response.status_code)

        data = response.json()
        articles = data.get("Data", [])

        logger.info("Number of articles fetched: %s", len(articles))

        return articles

    except requests.exceptions.RequestException as e:
        logger.error("Error fetching news: %s", e)
        return []

    except json.JSONDecodeError as e:
        logger.error("Error decoding JSON response: %s", e)
        return []


def save_news_to_minio(**context):
    """Saves news articles to a Minio bucket as a JSON file."""
    ti = context.get("ti")
    if not ti:
        logger.error("Task instance not found in context")
        return

    articles = ti.xcom_pull(task_ids="extract_news")

    if not articles:
        logger.warning("No articles to save to Minio.")
        return

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    bucket_name = settings.minio_bucket_raw_news
    object_name = f"{timestamp}_crypto_news.json"

    json_data = json.dumps(articles, ensure_ascii=False, indent=4).encode("utf-8")

    _endpoint = settings.minio_endpoint
    client = Minio(
        _endpoint.replace("http://", "").replace("https://", ""),
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=_endpoint.startswith("https://"),
    )

    try:
        if not client.bucket_exists(bucket_name):
            logger.info("Bucket '%s' does not exist. Creating it now.", bucket_name)
            client.make_bucket(bucket_name)

        client.put_object(
            bucket_name,
            object_name,
            data=io.BytesIO(json_data),
            length=len(json_data),
            content_type="application/json",
        )

        logger.info(
            "Successfully uploaded %s articles to Minio bucket '%s' as '%s'",
            len(articles),
            bucket_name,
            object_name,
        )

    except S3Error as e:
        logger.error("Minio S3 Error: %s", e)

    except Exception as e:
        logger.error("An unexpected error occurred while saving to Minio: %s", e)


def main():
    extracted_news = extract_news()
    dummy_ti = SimpleNamespace(xcom_pull=lambda task_ids=None: extracted_news)
    save_news_to_minio(ti=dummy_ti)


if __name__ == "__main__":
    main()
