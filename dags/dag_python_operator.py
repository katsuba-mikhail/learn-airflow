from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'learner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def hello(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f'Hello world! '
          f'My name is {first_name} {last_name}. '
          f'I am {age} years old.')

def get_name(ti):
    ti.xcom_push(key='first_name', value='Air')
    ti.xcom_push(key='last_name', value='Flow')

def get_age(ti):
    ti.xcom_push(key='age', value=22)

with DAG(
    default_args=default_args,
    dag_id='dag_python_operator_v6',
    start_date=datetime(2023, 4, 12),
    schedule_interval='@daily',
    catchup=False,

) as dag:

    task1 = PythonOperator(
        task_id='hello',
        python_callable=hello,
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
    )

    [task2, task3] >> task1