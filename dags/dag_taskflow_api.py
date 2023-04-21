from datetime import datetime, timedelta
from airflow.decorators import dag, task


default_arg = {
    'owner': 'learner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='dag_taskflow_api_v1',
    default_args=default_arg,
    start_date=datetime(2023, 4, 12),
    schedule_interval='@daily',
    catchup=False
)

def hello_world_etl():
    
    @task()
    def get_name():
        return 'AirFlow'
    
    @task()
    def get_age():
        return 22
    
    @task()
    def hello(name, age):
        print(f'Hello World! My name is {name}, I am {age} years old')

    name = get_name()
    age = get_age()
    hello(name=name, age=age)

hello_dag = hello_world_etl()