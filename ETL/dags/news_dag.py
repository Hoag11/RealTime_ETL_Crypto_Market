import datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from scripts.batch import news_crawler

with DAG(
    dag_id="news_dag",
    start_date=datetime.datetime(2025, 9, 16),
    schedule="0 7 * * 1",
    catchup=False,
    tags=["news", "batch"],
) as dag:
    extract_news = PythonOperator(
        task_id="extract_news",
        python_callable=news_crawler.extract_news,
    )
    save_news_to_minio = PythonOperator(
        task_id="save_news_to_minio",
        python_callable=news_crawler.save_news_to_minio,
        provide_context=True,
    )

    extract_news >> save_news_to_minio
