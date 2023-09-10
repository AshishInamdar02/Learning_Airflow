from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'ashish',
    'retries': '2',
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='Postgres_Operator',
    default_args=default_args,
    start_date=datetime(2023, 8, 30, 2),
    schedule_interval='@once',
    catchup=False
) as dag:
    task1 = PostgresOperator(
        task_id='set_connection_and_create_table',
        postgres_conn_id='postgres_localhost',
        sql='sql/create_dag_log.sql'
    )

    task2 = PostgresOperator(
        task_id='insert_into_dag_log',
        postgres_conn_id='postgres_localhost',
        sql='sql/insert_dag_log.sql'
    )

    task1 >> task2
