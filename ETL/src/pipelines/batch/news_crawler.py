"""Fetch and persist crypto news articles."""
from __future__ import annotations

import io
import json
import logging
from datetime import datetime
from typing import Iterable, Optional

import requests
from fake_useragent import UserAgent
from minio import Minio
from minio.error import S3Error

from pipelines import config

ua = UserAgent()
API_BATCHING = config.API_BATCHING
MINIO_ENDPOINT = config.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = config.MINIO_SECRET_KEY
BUCKET_NAME = config.MINIO_BUCKET_RAW_NEWS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _init_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=config.MINIO_ENDPOINT.startswith("https"),
    )


def extract_news() -> list[dict]:
    """Fetches news articles from the remote API."""
    try:
        response = requests.get(
            API_BATCHING,
            headers={"User-Agent": ua.random},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        articles: Iterable[dict] = data.get("Data", []) if isinstance(data, dict) else []
        articles_list = list(articles)
        logging.info("Fetched %s articles", len(articles_list))
        return articles_list
    except requests.exceptions.RequestException as exc:
        logging.error("Error fetching news: %s", exc)
        return []
    except json.JSONDecodeError as exc:
        logging.error("Error decoding JSON response: %s", exc)
        return []


def save_news_to_minio(**context) -> None:
    """Saves news articles to a Minio bucket as a JSON file."""
    ti = context.get("ti")
    articles: Optional[list] = None
    if ti is not None:
        articles = ti.xcom_pull(task_ids="extract_news")
    if not articles:
        logging.warning("No articles to save to Minio.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"{timestamp}_crypto_news.json"
    json_data = json.dumps(articles, ensure_ascii=False, indent=4).encode("utf-8")

    client = _init_minio_client()
    try:
        if not client.bucket_exists(BUCKET_NAME):
            logging.info("Bucket '%s' does not exist. Creating it now.", BUCKET_NAME)
            client.make_bucket(BUCKET_NAME)

        client.put_object(
            BUCKET_NAME,
            object_name,
            data=io.BytesIO(json_data),
            length=len(json_data),
            content_type="application/json",
        )
        logging.info(
            "Successfully uploaded %s articles to Minio bucket '%s' as '%s'",
            len(articles),
            BUCKET_NAME,
            object_name,
        )
    except S3Error as exc:
        logging.error("Minio S3 Error: %s", exc)
    except Exception as exc:  # pragma: no cover - unexpected error handling
        logging.error("Unexpected error while saving to Minio: %s", exc)


def main() -> None:  # pragma: no cover - manual execution helper
    extracted_news = extract_news()
    save_news_to_minio(ti=type("TI", (), {"xcom_pull": lambda *_args, **_kwargs: extracted_news})())


if __name__ == "__main__":  # pragma: no cover
    main()
