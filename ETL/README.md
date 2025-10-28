# RealTime_ETL_Crypto_Market

This repository contains the data ingestion and processing jobs that power the
RealTime ETL Crypto Market platform. The code base includes Python scripts for
collecting raw data, Spark jobs for batch/stream processing and supporting
configuration files for the deployment stack.

## Configuration

Application settings are loaded from environment variables with convenient
fallbacks. For local development:

1. Copy the sample configuration and adjust it for your environment:

   ```bash
   cp .env.example .env
   ```

2. Export the project `src` directory so that utility modules are available when
   running scripts directly:

   ```bash
   export PYTHONPATH="$(pwd)/src"
   ```

   You can add the command above to your shell profile for convenience.

3. Override any value from the sample file by editing `.env` or by exporting the
   corresponding environment variable before launching a job.

The most common variables are:

- Kafka brokers and authentication (`KAFKA_*`).
- External API endpoints (`API_*`).
- MinIO connection details (`MINIO_*`).
- PostgreSQL warehouse credentials (`POSTGRES_*`).
- Spark cluster defaults (`SPARK_*`).

## Project layout

- `scripts/`: ad-hoc and scheduled ingestion scripts (streaming + batch).
- `spark/`: PySpark jobs for both streaming and batch loading into the
  warehouse.
- `configs/`: compatibility layer that exposes the runtime settings for legacy
  imports.
- `src/core/config.py`: single source of truth for application configuration.

Refer to comments in each script for usage instructions specific to that
module.
