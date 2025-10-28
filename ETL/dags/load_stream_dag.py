from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from pipelines.utils.spark import build_spark_command


def _get_runtime_value(name: str, default: str) -> str:
    candidates = [f"CRYPTO_ETL_{name}", name]
    for candidate in candidates:
        value = os.environ.get(candidate)
        if value:
            return value
    for candidate in candidates:
        try:
            return Variable.get(candidate)
        except KeyError:
            continue
    return default


def _default_project_dir() -> str:
    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    return os.path.join(airflow_home, "dags", "ETL")


PROJECT_DIR = _get_runtime_value("HOST_ETL_DIR", _default_project_dir())
SPARK_HOST_DIR = _get_runtime_value("HOST_SPARK_DIR", os.path.join(PROJECT_DIR, "spark"))
SRC_HOST_DIR = _get_runtime_value("HOST_SRC_DIR", os.path.join(PROJECT_DIR, "src"))
DOCKER_NETWORK = _get_runtime_value("DOCKER_NETWORK", "streaming-network-ez")
SPARK_IMAGE = _get_runtime_value("SPARK_DOCKER_IMAGE", "spark/spark:3.5")
IVY_VOLUME = _get_runtime_value("SPARK_IVY_VOLUME", "spark_lib")
DATA_VOLUME = _get_runtime_value("SPARK_DATA_VOLUME", "spark_data")
PYTHONPATH_CONTAINER = ["/opt/pipelines/src"]
SPARK_PACKAGES = [
    "org.postgresql:postgresql:42.7.3",
    "org.apache.hadoop:hadoop-aws:3.3.6",
]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_crypto_streaming_pipeline",
    description="Streaming ETL Pipeline: chạy Spark Structured Streaming container để load dữ liệu coin metrics vào Postgres",
    start_date=datetime(2025, 10, 21),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:
    volumes = [
        (SPARK_HOST_DIR, "/spark"),
        (SRC_HOST_DIR, "/opt/pipelines/src"),
        (IVY_VOLUME, "/opt/bitnami/spark/.ivy2"),
        (DATA_VOLUME, "/data"),
    ]

    spark_command = build_spark_command(
        job_name="fact_coin_metrics_streaming_job",
        script_path="/spark/spark/load/streaming_load.py",
        container_name="project-spark-stream",
        network=DOCKER_NETWORK,
        ui_port=4041,
        volumes=volumes,
        image=SPARK_IMAGE,
        packages=SPARK_PACKAGES,
        pythonpath=PYTHONPATH_CONTAINER,
    )

    load_fact_coin_metrics_stream = BashOperator(
        task_id="load_fact_coin_metrics_stream",
        bash_command=spark_command,
    )

    load_fact_coin_metrics_stream
