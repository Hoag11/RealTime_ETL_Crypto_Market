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
    dag_id="etl_crypto_batch_pipeline",
    description="Batch ETL Pipeline: load data into Postgres using Spark container",
    start_date=datetime(2025, 10, 1),
    schedule="0 8 * * 1-5",
    catchup=False,
    default_args=default_args,
) as dag:
    common_volumes = [
        (SPARK_HOST_DIR, "/spark"),
        (SRC_HOST_DIR, "/opt/pipelines/src"),
        (IVY_VOLUME, "/opt/bitnami/spark/.ivy2"),
        (DATA_VOLUME, "/data"),
    ]

    def _build_job(job_name: str, script_name: str, pip_packages: list[str] | None = None):
        return build_spark_command(
            job_name=job_name,
            script_path=f"/spark/spark/load/batch_loading/{script_name}.py",
            container_name=f"project-spark-{job_name}",
            network=DOCKER_NETWORK,
            ui_port=4040,
            volumes=common_volumes,
            image=SPARK_IMAGE,
            packages=SPARK_PACKAGES,
            pythonpath=PYTHONPATH_CONTAINER,
            pip_packages=pip_packages,
        )

    load_dim_exchanges = BashOperator(
        task_id="load_dim_exchanges",
        bash_command=_build_job(
            job_name="dim_exchanges_job",
            script_name="dim_exchanges",
            pip_packages=["psycopg2-binary"],
        ),
    )

    load_fact_market_global = BashOperator(
        task_id="load_fact_market_global",
        bash_command=_build_job(
            job_name="market_global_job",
            script_name="fact_market_global",
            pip_packages=["psycopg2-binary"],
        ),
    )

    load_fact_market_cap = BashOperator(
        task_id="load_fact_market_cap",
        bash_command=_build_job(
            job_name="market_cap_job",
            script_name="fact_market_cap",
            pip_packages=["psycopg2-binary"],
        ),
    )

    load_fact_market_cap_percentage = BashOperator(
        task_id="load_fact_market_cap_percentage",
        bash_command=_build_job(
            job_name="market_cap_percentage_job",
            script_name="fact_market_cap_percentage",
            pip_packages=["psycopg2-binary"],
        ),
    )

    load_fact_news = BashOperator(
        task_id="load_fact_news",
        bash_command=_build_job(
            job_name="fact_news_job",
            script_name="fact_news",
            pip_packages=["psycopg2-binary"],
        ),
    )

    load_fact_price_history = BashOperator(
        task_id="load_fact_price_history",
        bash_command=_build_job(
            job_name="price_history_job",
            script_name="fact_price_history",
            pip_packages=["psycopg2-binary"],
        ),
    )

    (
        load_dim_exchanges
        >> load_fact_market_global
        >> load_fact_market_cap
        >> load_fact_market_cap_percentage
        >> load_fact_news
        >> load_fact_price_history
    )
