from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

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
    SPARK_DOCKER_CMD = (
        "docker container stop project-spark >/dev/null 2>&1 || true && "
        "docker container rm project-spark >/dev/null 2>&1 || true && "
        'while [ "$(docker ps -aq -f name=project-spark)" ]; do '
        "  echo 'Chờ container cũ được xoá...'; sleep 1; "
        "done && "
        "docker run --rm --name project-spark "
        "--network streaming-network-ez "
        "-p 4040:4040 "
        "-v /home/hoang/Projects/RealTime_ETL_Crypto_Market/ETL/spark:/spark "
        "-v /home/hoang/Projects/RealTime_ETL_Crypto_Market/ETL/configs/:/configs/ "
        "-v spark_lib:/opt/bitnami/spark/.ivy2 "
        "-v spark_data:/data "
        "spark/spark:3.5 bash -c '"
        "pip install psycopg2-binary sqlalchemy numpy && "
        "export PYTHONPATH=$PYTHONPATH:/configs && "
        "spark-submit "
        "--packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.6 "
        "--executor-memory 2g --driver-memory 2g "
        "--name {job_name} "
        "/spark/spark/load/batch_loading/{script_name}.py'"
    )

    # -------------------- TASKS -------------------- #
    load_dim_exchanges = BashOperator(
        task_id="load_dim_exchanges",
        bash_command=SPARK_DOCKER_CMD.format(
            job_name="dim_exchanges_job",
            script_name="dim_exchanges",
        ),
    )

    load_fact_market_global = BashOperator(
        task_id="load_fact_market_global",
        bash_command=SPARK_DOCKER_CMD.format(
            job_name="market_global_job",
            script_name="fact_market_global",
        ),
    )

    load_fact_market_cap = BashOperator(
        task_id="load_fact_market_cap",
        bash_command=SPARK_DOCKER_CMD.format(
            job_name="market_cap_job",
            script_name="fact_market_cap",
        ),
    )

    load_fact_market_cap_percentage = BashOperator(
        task_id="load_fact_market_cap_percentage",
        bash_command=SPARK_DOCKER_CMD.format(
            job_name="market_cap_percentage_job",
            script_name="fact_market_cap_percentage",
        ),
    )

    load_fact_volume = BashOperator(
        task_id="load_fact_volume",
        bash_command=SPARK_DOCKER_CMD.format(
            job_name="fact_volume_job",
            script_name="fact_market_volume",
        ),
    )

    load_fact_news = BashOperator(
        task_id="load_fact_news",
        bash_command=SPARK_DOCKER_CMD.format(
            job_name="fact_news_job",
            script_name="fact_news",
        ),
    )

    load_fact_price_history = BashOperator(
        task_id="load_fact_price_history",
        bash_command=SPARK_DOCKER_CMD.format(
            job_name="price_history_job",
            script_name="fact_price_history",
        ),
    )

    # -------------------- DAG FLOW -------------------- #
    (
        load_dim_exchanges
        >> load_fact_market_global
        >> load_fact_market_cap
        >> load_fact_market_cap_percentage
        >> load_fact_volume
        >> load_fact_news
        >> load_fact_price_history
    )
