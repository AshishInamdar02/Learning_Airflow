from datetime import datetime, timedelta
import csv
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile

default_args = {
    'owner': 'ashish',
    'retries': '2',
    'retry_delay': timedelta(minutes=2),

}


def get_data_from_postgres(ds_nodash):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute('select * from dag_logs')
    with NamedTemporaryFile(mode='w', suffix=f'{ds_nodash}') as f:
        # with open(f'dags/airflow_logs_{ds_nodash}.txt', 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)

        cursor.close()
        conn.close()

        logging.info("Saved data to text file %s.",
                     f'dags/airflow_logs_{ds_nodash}.txt')

        s3_hook = S3Hook(aws_conn_id='s3_connection')
        s3_hook.load_file(
            filename=f.name,
            key=f'logs/{ds_nodash}.txt',
            bucket_name='airflow-demo-2023-09-18',
            replace=True
        )


'''
def push_data_to_s3(ds_nodash):
    s3_hook = S3Hook(aws_conn_id='s3_connection')
    s3_hook.load_file(
        filename=f'dags/airflow_logs_{ds_nodash}.txt',
        key=f'logs/{ds_nodash}.txt',
        bucket_name='airflow-demo-2023-09-18',
        replace=True
    )
'''

with DAG(
    dag_id='Postgres_Hook',
    default_args=default_args,
    description='DAG for Postgres Hook',
    start_date=datetime(2003, 9, 18),
    schedule_interval='@once',
    catchup=False

) as dag:
    task1 = PythonOperator(
        task_id='Get_Data_from_Postgres',
        python_callable=get_data_from_postgres
    )
    task1
'''
    task2 = PythonOperator(
        task_id='Push_Data_to_S3',
        python_callable=push_data_to_s3
    )
'''

# >> task2
