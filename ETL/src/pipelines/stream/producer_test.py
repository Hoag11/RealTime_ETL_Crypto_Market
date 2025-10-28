"""Simple utility to test the Kafka producer locally."""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional

import requests
from fake_useragent import UserAgent
from kafka import KafkaProducer

from pipelines import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
ua = UserAgent()


def get_data_from_api() -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(
            config.API_STREAMING,
            headers={"User-Agent": ua.random},
            timeout=15,
        )
        response.raise_for_status()
        logging.info("Data fetched successfully from %s", config.API_STREAMING)
        payload = response.json()
        return payload if isinstance(payload, dict) else None
    except requests.exceptions.RequestException as exc:
        logging.error("Error fetching data from API: %s", exc)
        return None


def create_kafka_producer() -> None:
    producer = KafkaProducer(
        bootstrap_servers=config.KAKFA_BROKER,
        security_protocol=config.KAFKA_SECURITY_PROCTOCOL,
        sasl_mechanism=config.KAFKA_SASL_MECHANISM,
        sasl_plain_username=config.KAFKA_SASL_USERNAME,
        sasl_plain_password=config.KAFKA_SASL_PASSWORD,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    try:
        while True:
            data = get_data_from_api()
            if data:
                producer.send(config.KAFKA_TOPIC, value=data)
                logging.info("Data sent to Kafka topic %s", config.KAFKA_TOPIC)
            else:
                logging.warning("No data to send to Kafka")

            producer.flush()
            time.sleep(2)
    except KeyboardInterrupt:  # pragma: no cover - manual interrupt handling
        logging.info("Kafka producer stopped manually")
    except Exception as exc:  # pragma: no cover - defensive logging
        logging.error("Error in Kafka producer: %s", exc)
    finally:
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed")


if __name__ == "__main__":  # pragma: no cover
    create_kafka_producer()
