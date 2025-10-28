import json
import logging
import time
from typing import Any, Optional, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError
from requests import RequestException, Response

from configs import configs
from scripts.common import http_client

# logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_data_from_api() -> Tuple[Optional[Any], Optional[Response]]:
    try:
        response = http_client.get(
            configs.API_STREAMING, timeout=configs.STREAM_REQUEST_TIMEOUT
        )
        logging.info(
            "Data fetched successfully from %s in %.2fs",
            configs.API_STREAMING,
            response.elapsed.total_seconds(),
        )
        return response.json(), response
    except RequestException as exc:
        logging.error("Error fetching data from API: %s", exc)
        return None, None


def send_with_retry(producer: KafkaProducer, payload: Any) -> None:
    last_error: Optional[Exception] = None
    for attempt in range(1, configs.STREAM_KAFKA_SEND_RETRIES + 1):
        future = producer.send(configs.KAFKA_TOPIC, value=payload)
        try:
            future.get(timeout=configs.KAFKA_SEND_TIMEOUT)
            logging.info(
                "Data sent to Kafka topic %s", configs.KAFKA_TOPIC
            )
            return
        except KafkaError as exc:
            logging.warning(
                "Kafka send failed (attempt %s/%s): %s",
                attempt,
                configs.STREAM_KAFKA_SEND_RETRIES,
                exc,
            )
            last_error = exc
        except Exception as exc:  # pragma: no cover - unexpected errors
            logging.warning(
                "Unexpected error while sending to Kafka (attempt %s/%s): %s",
                attempt,
                configs.STREAM_KAFKA_SEND_RETRIES,
                exc,
            )
            last_error = exc

        if attempt < configs.STREAM_KAFKA_SEND_RETRIES:
            backoff = min(
                configs.STREAM_MAX_SLEEP,
                configs.STREAM_KAFKA_SEND_BACKOFF * attempt,
            )
            logging.info("Retrying Kafka send in %.2fs", backoff)
            time.sleep(backoff)

    if last_error:
        raise last_error
    raise RuntimeError("Failed to deliver message to Kafka after retries")


def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=configs.KAKFA_BROKER,
        security_protocol=configs.KAFKA_SECURITY_PROCTOCOL,
        sasl_mechanism=configs.KAFKA_SASL_MECHANISM,
        sasl_plain_username=configs.KAFKA_SASL_USERNAME,
        sasl_plain_password=configs.KAFKA_SASL_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_in_flight_requests_per_connection=configs.KAFKA_MAX_IN_FLIGHT,
        retries=configs.KAFKA_PRODUCER_RETRIES,
        retry_backoff_ms=configs.KAFKA_RETRY_BACKOFF_MS,
    )

    try:
        while True:
            cycle_start = time.monotonic()
            payload, response = get_data_from_api()

            if payload:
                send_with_retry(producer, payload)
            else:
                logging.warning("No data to send to Kafka")

            producer.flush()

            request_duration = (
                response.elapsed.total_seconds() if response is not None else 0.0
            )
            elapsed = time.monotonic() - cycle_start
            base_sleep = max(
                0.0,
                configs.STREAM_TARGET_POLL_INTERVAL - request_duration,
            )
            sleep_time = min(
                configs.STREAM_MAX_SLEEP,
                max(configs.STREAM_MIN_POLL_INTERVAL, base_sleep),
            )

            if response is not None:
                rate_limit_reset = response.headers.get("X-RateLimit-Reset")
                if rate_limit_reset:
                    try:
                        reset_in = float(rate_limit_reset)
                        sleep_time = max(sleep_time, min(reset_in, configs.STREAM_MAX_SLEEP))
                    except ValueError:
                        pass

            logging.debug(
                "Cycle took %.2fs (request: %.2fs). Sleeping %.2fs",
                elapsed,
                request_duration,
                sleep_time,
            )

            if sleep_time > 0:
                time.sleep(sleep_time)
    except KeyboardInterrupt:
        logging.info("Kafka producer stopped manually")
    except Exception as e:
        logging.error(f"Error in Kafka producer: {e}")
        raise
    finally:
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed")


if __name__ == "__main__":
    create_kafka_producer()
