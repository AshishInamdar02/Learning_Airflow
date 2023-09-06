from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'ashish',
    'retries': '2',
    'retry_delay': timedelta(minutes=2)

}


with DAG(
    dag_id='first_dag',
    default_args=default_args,
    description='This is the first dag',
    start_date=datetime(2023, 8, 30, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo First Task'
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo Second Task'
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo Third Task'
    )

    task1 >> task2
    task1 >> task3
