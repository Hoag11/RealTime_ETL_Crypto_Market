# Real-Time ETL Crypto Market

This repository contains the infrastructure and jobs that power the streaming and batch
ingestion pipelines for the crypto market analytics platform. The stack leverages Kafka,
Spark, Airflow and MinIO to collect, transform and serve data in near real-time.

The documentation below explains how to set up a local development environment, execute
automated tests, deploy the services and configure the new monitoring & alerting
capabilities.

## Prerequisites

- Python 3.10+
- Java 8+ (required by PySpark)
- Docker and Docker Compose (for running the infrastructure locally)

## Install dependencies

Create a virtual environment and install the runtime packages:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For development and CI tasks (linting, testing) install the extra tooling:

```bash
pip install -r requirements-dev.txt
```

## Running tests and linting

Automated validation is based on **pytest** and **Ruff**. Tests cover the new MinIO
payload validation helper and the Spark transformation routines that cast and enrich coin
records.

Run linting:

```bash
ruff check .
```

Run the test suite:

```bash
pytest
```

Both commands should be executed from the `ETL` directory with the virtual environment
activated.

### Continuous Integration

GitHub Actions automatically runs the same lint and test commands on every push and pull
request. The workflow definition lives in `.github/workflows/ci.yml` and installs the
development requirements before executing the checks.

## Deploying the pipeline

1. **Infrastructure** – Use `docker-compose.yml` (and the additional services in the
   `docker/` directory) to start Kafka, Spark, Airflow, MinIO and other dependencies:
   ```bash
   docker compose up --build
   ```
   Ensure that all containers share the same Docker network so Spark can access Kafka and
   MinIO using their service names.

2. **Airflow DAGs** – Mount the `dags/` folder into the Airflow scheduler/webserver
   container. The DAG `global_market_data_pipeline` uses the validated payload helper and
   writes results to MinIO. Set the following environment variable in Airflow to enable
   Slack alerts:
   ```bash
   export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/XXXX/XXXX/XXXX"
   ```
   Without the variable, Airflow will log a warning and skip the Slack notification.

3. **Streaming jobs** – Build the Spark container defined in `spark/docker-compose.yml`.
   The updated `consumer_price.py` module exposes reusable functions to parse Kafka
   payloads, transform records and export Prometheus metrics.

## Monitoring & alerting

### Airflow notifications

The DAG default arguments now include `on_failure_callback=notify_failure`. When a task
fails and the `SLACK_WEBHOOK_URL` environment variable is configured, a Slack message is
sent with the DAG, task and run details. The helper is implemented in
`dags/utils/notifications.py`.

### Prometheus metrics for Spark streaming

`spark/spark/consumer_price.py` exposes basic Prometheus metrics:

- `spark_stream_processed_records_total` – counts the number of records processed across
  completed micro-batches.
- `spark_stream_last_trigger_duration_seconds` – stores the execution duration of the
  most recent micro-batch.

The script starts an HTTP server on port `8000` when run as a standalone streaming job.
Scrape this endpoint from Prometheus and visualize the metrics in Grafana.

### Local metric checks

During development you can run the Spark consumer locally and inspect the metrics:

```bash
PYSPARK_PYTHON=python3 spark-submit spark/spark/consumer_price.py
# Visit http://localhost:8000/metrics to view the exported metrics
```

## Troubleshooting

- **Missing Slack alerts** – confirm the webhook environment variable is set and reachable.
- **PySpark fails to start** – ensure Java is installed and `JAVA_HOME` is configured.
- **Tests fail locally** – clear the `spark-warehouse/` directory if a previous Spark
  session left stale metadata.

## Repository structure (excerpt)

```
ETL/
├── dags/
│   ├── utils/notifications.py  # Slack notifier helpers
│   └── global_data_dag.py      # Airflow DAG with retry & alert settings
├── scripts/batch/global_market.py  # MinIO upload & payload validation
├── spark/spark/consumer_price.py   # Streaming ingestion & Prometheus metrics
├── tests/                          # Pytest-based unit tests
└── requirements-dev.txt            # Tooling for linting & tests
```

The documentation will evolve as more ETL components are onboarded. Contributions are
welcome—please run the lint and test commands before opening a pull request.
