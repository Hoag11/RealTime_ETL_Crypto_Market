from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql


def create_database():
    try:
        conn = psycopg2.connect(
            host="postgres-bi",
            port=5432,
            user="postgres",
            password="postgres",
            database="postgres",
        )
        conn.autocommit = True  # Để tạo database, cần bật autocommit
        cursor = conn.cursor()

        # Kiểm tra xem database stockwh đã tồn tại chưa
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'warehouse'")
        exists = cursor.fetchone()
        if not exists:
            # Tạo database stockwh nếu chưa tồn tại
            cursor.execute(sql.SQL("CREATE DATABASE warehouse"))
            print("Database warehouse created successfully.")
        else:
            print("Database warehouse already exists.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")
        raise


# Định nghĩa DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "init_db_dag",
    default_args=default_args,
    description="DAG to initialize the stockwh database, schemas, and tables",
    schedule="@once",  # Chỉ chạy một lần khi Airflow khởi động
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    # Task 1: Tạo database stockwh
    create_db_task = PythonOperator(
        task_id="create_database",
        python_callable=create_database,
    )
    create_wh_schema_task = SQLExecuteQueryOperator(
        task_id="create_wh_schema",
        conn_id="postgres",
        sql="/scripts/sql/warehouse.sql",
        database="warehouse",
    )

    init_dim_date = SQLExecuteQueryOperator(
        task_id="init_dim_date",
        conn_id="postgres",
        sql="/scripts/sql/dim_date.sql",
        database="warehouse",
    )

    init_dim_concurencies = SQLExecuteQueryOperator(
        task_id="init_dim_concurrencies",
        conn_id="postgres",
        sql="/scripts/sql/concurencies.sql",
        database="warehouse",
    )
    # Định nghĩa thứ tự chạy
    create_db_task >> create_wh_schema_task >> init_dim_date >> init_dim_concurencies
