from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipelines.batch import exchange_list

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="exchange_list_data_pipeline",
    default_args=default_args,
    description="Fetch and save exchange list data to Minio",
    schedule="0 6 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "exchange"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_exchange_data",
        python_callable=exchange_list.fetch_data,
    )

    save_task = PythonOperator(
        task_id="save_exchange_to_minio",
        python_callable=exchange_list.get_data,
    )

    fetch_task >> save_task
