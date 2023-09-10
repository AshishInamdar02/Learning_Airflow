from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'ashish',
    'retries': '2',
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='Catchup_and_Backfill',
    default_args=default_args,
    description='This DAG demonstrates Catchup and Backfill.',
    start_date=datetime(2023, 8, 30, 2),
    schedule='@daily',
    # catchup=True
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='ECHO',
        bash_command='echo This is for Catchup and Backfill.'
        # docker exex -it dockerId bach
        # airflow dags backfill -s <start_date> -e <end_date> <DAG_Name>
    )
