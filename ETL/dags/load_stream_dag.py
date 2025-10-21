from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

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
    # -------------------- SPARK DOCKER CMD -------------------- #
    SPARK_DOCKER_CMD = (
        # 🧹 Dọn container cũ nếu còn
        "docker container stop project-spark-stream >/dev/null 2>&1 || true && "
        "docker container rm project-spark-stream >/dev/null 2>&1 || true && "
        'while [ "$(docker ps -aq -f name=project-spark-stream)" ]; do '
        "  echo 'Chờ container cũ được xoá...'; sleep 1; "
        "done && "
        # 🚀 Chạy container Spark
        "docker run --rm --name project-spark-stream "
        "--network streaming-network-ez "
        "-p 4041:4040 "  # Cổng web UI khác để tránh trùng batch
        "-v /home/hoang/Projects/RealTime_ETL_Crypto_Market/ETL/spark:/spark "
        "-v /home/hoang/Projects/RealTime_ETL_Crypto_Market/ETL/configs/:/configs/ "
        "-v spark_lib:/opt/bitnami/spark/.ivy2 "
        "-v spark_data:/data "
        "spark/spark:3.5 bash -c '"
        "export PYTHONPATH=$PYTHONPATH:/configs && "
        "spark-submit "
        "--packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.6 "
        "--executor-memory 2g --driver-memory 2g "
        "--name {job_name} "
        "/spark/spark/load/{script_name}.py'"
    )

    # -------------------- TASK -------------------- #
    load_fact_coin_metrics_stream = BashOperator(
        task_id="load_fact_coin_metrics_stream",
        bash_command=SPARK_DOCKER_CMD.format(
            job_name="fact_coin_metrics_streaming_job",
            script_name="streaming_load",
        ),
    )

    load_fact_coin_metrics_stream
