from datetime import datetime, timedelta
from airflow.decorators import dag, task


default_arg = {
    'owner': 'learner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='dag_taskflow_api_v2',
    default_args=default_arg,
    start_date=datetime(2023, 4, 12),
    schedule_interval='@daily',
    catchup=False
)

def hello_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Air',
            'last_name': 'Flow'
        }
    
    @task()
    def get_age():
        return 22
    
    @task()
    def hello(first_name, last_name, age):
        print(f'Hello World! My name is {first_name} {last_name},'
              f'I am {age} years old')

    name_dict = get_name()
    age = get_age()
    hello(first_name=name_dict['first_name'], 
          last_name=name_dict['last_name'],
          age=age)

hello_dag = hello_world_etl()