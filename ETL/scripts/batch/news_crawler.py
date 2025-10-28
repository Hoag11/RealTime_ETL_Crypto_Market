import os
import sys


CURRENT_DIR = os.path.dirname(__file__)
SCRIPTS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPTS_DIR, ".."))

for path in (PROJECT_ROOT, SCRIPTS_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

import requests
import logging
import json
from datetime import datetime, timezone
from fake_useragent import UserAgent

from configs import configs
from minio.error import S3Error

from scripts.helpers.minio_utils import (
    ensure_bucket,
    get_minio_client,
    upload_json_bytes,
)

ua = UserAgent()

# Configure logging to see messages in the console
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_news():
    """Fetches news articles from a remote API."""
    try:
        response = requests.get(
            configs.API_BATCHING, headers={"User-Agent": ua.random}, timeout=10
        )
        response.raise_for_status()  # This will raise an HTTPError for bad responses (4xx or 5xx)
        logging.info(f"Response status code: {response.status_code}")

        data = response.json()
        articles = data.get("Data", [])

        logging.info(f"Number of articles fetched: {len(articles)}")

        return articles

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching news: {e}")
        return []

    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON response: {e}")
        return []


def save_news_to_minio(articles=None, **context):
    """Saves news articles to a Minio bucket as a JSON file."""
    if articles is None:
        ti = context.get("ti")  # Or context['task_instance']
        if ti:
            articles = ti.xcom_pull(task_ids="extract_news")  # Pull from upstream task

    if not articles:
        logging.warning("No articles to save to Minio.")
        return

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    date_partition = now.strftime("%Y-%m-%d")
    bucket_name = configs.MINIO_BUCKET_RAW_NEWS
    object_name = f"news/date={date_partition}/{timestamp}_crypto_news.json"

    # Create an in-memory file-like object to avoid writing to disk
    json_data = json.dumps(articles, ensure_ascii=False, indent=4).encode("utf-8")

    try:
        client = get_minio_client(
            configs.MINIO_ENDPOINT,
            configs.MINIO_ACCESS_KEY,
            configs.MINIO_SECRET_KEY,
            secure=False,
        )

        ensure_bucket(client, bucket_name)

        upload_json_bytes(client, bucket_name, object_name, json_data)

        logging.info(
            f"Successfully uploaded {len(articles)} articles to Minio bucket '{bucket_name}' as '{object_name}'"
        )

    except S3Error as e:
        logging.error(f"Minio S3 Error: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred while saving to Minio: {e}")


def main():
    extracted_news = extract_news()
    save_news_to_minio(articles=extracted_news)


if __name__ == "__main__":
    main()
