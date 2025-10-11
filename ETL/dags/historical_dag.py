from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.batch import historical

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="historical_data_pipeline",
    default_args=default_args,
    description="Fetch and save historical OHLC data to Minio",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "historical"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_historical_data",
        python_callable=historical.fetch_historical_data,
    )

    save_task = PythonOperator(
        task_id="save_historical_to_minio",
        python_callable=historical.save_historical_to_minio,
    )

    fetch_task >> save_task
