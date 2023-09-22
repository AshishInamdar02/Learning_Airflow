from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'ashish',
    'retries': '2',
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='DAG_for_S3_Sensor',
    start_date=datetime(2023, 9, 18),
    schedule_interval='@once',
    default_args=default_args,
    catchup=False
) as dag:
    task1 = S3KeySensor(
        task_id='sense_S3',
        bucket_name='airflow-demo-2023-09-18',
        bucket_key='data.csv',
        aws_conn_id='s3_connection',
        mode='poke',
        poke_interval=5,
        timeout=30
    )
