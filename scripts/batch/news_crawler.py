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


def save_news_to_minio(**context):
    """Saves news articles to a Minio bucket as a JSON file."""
    ti = context["ti"]  # Or context['task_instance']
    articles = ti.xcom_pull(task_ids="extract_news")  # Pull from upstream task

    if not articles:
        logging.warning("No articles to save to Minio.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bucket_name = configs.MINIO_BUCKET_RAW_NEWS
    object_name = f"{timestamp}_crypto_news.json"

    # Create an in-memory file-like object to avoid writing to disk
    json_data = json.dumps(articles, ensure_ascii=False, indent=4).encode("utf-8")

    try:
        client = Minio(
            configs.MINIO_ENDPOINT.replace("http://", ""),
            access_key=configs.MINIO_ACCESS_KEY,
            secret_key=configs.MINIO_SECRET_KEY,
            secure=False,
        )

        # Check if bucket exists and create if it doesn't
        if not client.bucket_exists(bucket_name):
            logging.info(f"Bucket '{bucket_name}' does not exist. Creating it now.")
            client.make_bucket(bucket_name)

        # Upload the in-memory data
        client.put_object(
            bucket_name,
            object_name,
            data=io.BytesIO(json_data),
            length=len(json_data),
            content_type="application/json",
        )

        logging.info(
            f"Successfully uploaded {len(articles)} articles to Minio bucket '{bucket_name}' as '{object_name}'"
        )

    except S3Error as e:
        logging.error(f"Minio S3 Error: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred while saving to Minio: {e}")


def main():
    extracted_news = extract_news()
    save_news_to_minio(extracted_news)


if __name__ == "__main__":
    main()
