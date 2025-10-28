import json
import logging
import sys
import time
from pathlib import Path

import requests
from fake_useragent import UserAgent
from kafka import KafkaProducer

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


def get_data_from_api():
    try:
        response = requests.get(
            settings.api_streaming_url, headers={"User-Agent": ua.random}
        )
        response.raise_for_status()
        logger.info("Data fetched successfully from %s", settings.api_streaming_url)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("Error fetching data from API: %s", e)
        return None


def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_brokers,
        security_protocol=settings.kafka_security_protocol,
        sasl_mechanism=settings.kafka_sasl_mechanism,
        sasl_plain_username=settings.kafka_sasl_username,
        sasl_plain_password=settings.kafka_sasl_password,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        while True:
            data = get_data_from_api()
            if data:
                producer.send(settings.kafka_topic, value=data)
                logger.info("Data sent to Kafka topic %s", settings.kafka_topic)
            else:
                logger.warning("No data to send to Kafka")

            producer.flush()
            time.sleep(2)
    except KeyboardInterrupt:
        logger.info("Kafka producer stopped manually")
    except Exception as e:
        logger.error("Error in Kafka producer: %s", e)
    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed")


if __name__ == "__main__":
    create_kafka_producer()
