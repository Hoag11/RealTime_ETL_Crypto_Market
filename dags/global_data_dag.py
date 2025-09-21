from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.batch import global_market

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="global_market_data_pipeline",
    default_args=default_args,
    description="Fetch and save global market data to Minio",
    schedule="@daily",  # chạy hằng ngày
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "global_market"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_global_market_data",
        python_callable=global_market.fetch_data,
    )

    save_task = PythonOperator(
        task_id="save_global_to_minio",
        python_callable=global_market.get_data,
    )

    fetch_task >> save_task
