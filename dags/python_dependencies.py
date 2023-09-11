from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'coder2j',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

# Run docker build . --tag extending_airflow:latest
# Run docker compose up -d --no-deps --build airflow-webserver airflow-scheduler


def get_pyspark():
    import pyspark
    print(f"PySpark with version: {pyspark.__version__} ")


with DAG(
    dag_id='Python_Dependencies',
    description='Installing Python Dependencies with a requirement file.',
    start_date=datetime(2023, 8, 31),
    schedule_interval='@once'
) as dag:
    task1 = PythonOperator(
        task_id='Show_PySpark_Version.',
        python_callable=get_pyspark
    )

    task1
