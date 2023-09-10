from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'ashish',
    'retries': '2',
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='Cron_DAG',
    default_args=default_args,
    description='This is the first dag',
    start_date=datetime(2023, 8, 30, 2),
    # The cron expression is made of five fields. Each field can have the following values.
    # *	* *	* *
    # minute (0-59)	hour (0 - 23)	day of the month (1 - 31)	month (1 - 12)	day of the week (0 - 6)
    # At 30 minutes past the hour, every 6 hours, on day 2 of the month, and on Saturday, only in July
    schedule_interval='30 */6 2 7 SAT'
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
