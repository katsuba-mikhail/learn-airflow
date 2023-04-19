from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'learner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def hello(name, age):
    print(f'Hello world!'
          f'My name is {name}'
          f'I am {age} years old.')

with DAG(
    default_args=default_args,
    dag_id='dag_python_operator_v2',
    start_date=datetime(2023, 4, 12),
    schedule_interval='@daily',
    catchup=False,

) as dag:

    task1 = PythonOperator(
        task_id='hello',
        python_callable=hello,
        op_kwargs={'name': 'Hihi', 'age': '22'}
    )

    task1