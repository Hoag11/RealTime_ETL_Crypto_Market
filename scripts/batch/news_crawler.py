import requests
import os
import logging
import json
from fake_useragent import UserAgent
from datetime import datetime
from configs import configs

# logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


ua = UserAgent()


def extract_news():
    try:
        response = requests.get(configs.API_BATCHING, headers={
                                "User-Agent": ua.random})
        logging.info(f"Response status code: {response.status_code}")

        data = response.json()
        articles = data.get("Data", [])

        logging.info(f"Number of articles fetched: {len(articles)}")

        return articles

    except Exception as e:
        logging.error(f"Error fetching news: {e}")
        return []


def save_news(articles):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    project_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    data_dir = os.path.join(project_root, "data")

    os.makedirs(data_dir, exist_ok=True)

    out_put_name = f"{timestamp}_crypto_news.json"
    output_path = os.path.join(data_dir, out_put_name)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=4)
    logging.info(f"Saved {len(articles)} articles to {output_path}")


if __name__ == "__main__":
    logging.info("Starting news extraction process")
    articles = extract_news()
    save_news(articles)
