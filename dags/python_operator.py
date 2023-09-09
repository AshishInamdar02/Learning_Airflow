from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'ashish',
    'retries': '2',
    'retry_delay': timedelta(minutes=2)
}

# Baisc Python Function


def greet():
    print("This is an example of the Python Operator DAG.")

# Python Function Accepting Parameters


def parameter_greet(name):
    print(f"Hi {name}")

# Python Function returning value


def get_name(ti):
    ti.xcom_push(key='name', value='Ashish')


def get_name_xcom(ti):
    name_xcom = ti.xcom_pull(task_ids='XCOM_Example', key='name')
    print(f"Hi {name_xcom}. This was pulled from XCOM.")


with DAG(
    dag_id='Python_Operator',
    default_args=default_args,
    description='Example for Python Operator',
    start_date=datetime(2023, 8, 30, 2),
    schedule_interval='@daily'

) as dag:

    task1 = PythonOperator(
        task_id='Greet',
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id='Greet_Parameter',
        python_callable=parameter_greet,
        op_kwargs={'name': 'Ashish'}
    )

    task3 = PythonOperator(
        task_id='XCOM_Example',
        python_callable=get_name
    )

    task4 = PythonOperator(
        task_id='Greet_with_XCOM',
        python_callable=get_name_xcom
    )

    task1
    task2
    task3 >> task4
