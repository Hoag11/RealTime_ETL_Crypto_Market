from configs import configs
from kafka import KafkaProducer
from fake_useragent import UserAgent
import requests
import json
import logging
import time

# logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

ua = UserAgent()


def get_data_from_api():
    try:
        response = requests.get(
            configs.API_STREAMING, headers={"User-Agent": ua.random}
        )
        response.raise_for_status()
        logging.info(f"Data fetched successfully from {configs.API_STREAMING}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=configs.KAKFA_BROKER,
        security_protocol=configs.KAFKA_SECURITY_PROCTOCOL,
        sasl_mechanism=configs.KAFKA_SASL_MECHANISM,
        sasl_plain_username=configs.KAFKA_SASL_USERNAME,
        sasl_plain_password=configs.KAFKA_SASL_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        while True:
            data = get_data_from_api()
            if data:
                producer.send(configs.KAFKA_TOPIC, value=data)
                logging.info(f"Data sent to Kafka topic {configs.KAFKA_TOPIC}")
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
