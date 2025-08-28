from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.batch import news_crawler


def extract_news():
    news_crawler.extract_news()


def save_news():
    news_crawler.save_news()


with DAG(
    'crawler_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 28),
        'retries': 3,
        'execution_timeout': timedelta(minutes=10),
    },
    description='A DAG to run the web crawler every 6 hours',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['crawler'],
) as dag:
    crawler_task = PythonOperator(
        task_id='run_crawler',
        python_callable: extract_news
    )

    save_news = PythonOperator(
        task_id='save_news',
        python_callable: save_news
    )


crawler_task >> save_news
