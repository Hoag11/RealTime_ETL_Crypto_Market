import json
import logging
import time

import requests
from fake_useragent import UserAgent
from kafka import KafkaProducer


ua = UserAgent()

# Hardcoded configuration
KAFKA_BROKER = ["localhost:9094", "localhost:9194", "localhost:9294"]
KAFKA_TOPIC = "coin_prices"
KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_USERNAME = "admin"
KAFKA_SASL_PASSWORD = "Unigap@2024"
API_STREAMING = "https://api.coinlore.net/api/tickers/"


def get_data_from_api():
    try:
        response = requests.get(API_STREAMING, headers={
                                "User-Agent": ua.random})
        response.raise_for_status()
        logging.info(f"Data fetched successfully from {API_STREAMING}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_SASL_USERNAME,
        sasl_plain_password=KAFKA_SASL_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        while True:
            data = get_data_from_api()
            if data:
                producer.send(KAFKA_TOPIC, value=data)
                logging.info(f"Data sent to Kafka topic {KAFKA_TOPIC}")
            else:
                logging.warning("No data to send to Kafka")

            producer.flush()
            time.sleep(2)
    except KeyboardInterrupt:
        logging.info("Kafka producer stopped manually")
    except Exception as e:
        logging.error(f"Error in Kafka producer: {e}")
    finally:
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed")


if __name__ == "__main__":
    create_kafka_producer()
