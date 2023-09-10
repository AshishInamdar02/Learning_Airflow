from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner': 'ashish',
    'retries': '2',
    'retry_delay': timedelta(minutes=2)
}


@dag(dag_id='DAG_with_TaskFlow_API',
     default_args=default_args,
     description='Example for Task Flow API',
     start_date=datetime(2023, 8, 30, 2),
     schedule_interval='@daily')
def hello():

    # Setting the value normally
    @task()
    def get_name():
        return 'Ashish'

    # Passing multiple values
    @task()
    def get_fullname(multiple_outputs=True):
        return {'firstname': 'Ashish', 'lastname': 'Inamdar'}

    @task()
    def greet(name):
        print(f"Hello {name}")

    @task()
    def greet_multiple(firstname, lastname):
        print(f"Hi {firstname} {lastname}")

    name = get_name()
    greet(name=name)

    fullname_dict = get_fullname()
    greet_multiple(
        firstname=fullname_dict['firstname'], lastname=fullname_dict['lastname'])


taskflow_api_dag = hello()
