from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'learner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def hello(age, ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    print(f'Hello world! '
          f'My name is {first_name} {last_name}. '
          f'I am {age} years old.')

def get_name(ti):
    ti.xcom_push(key='first_name', value='Air')
    ti.xcom_push(key='last_name', value='Flow')

with DAG(
    default_args=default_args,
    dag_id='dag_python_operator_v5',
    start_date=datetime(2023, 4, 12),
    schedule_interval='@daily',
    catchup=False,

) as dag:

    task1 = PythonOperator(
        task_id='hello',
        python_callable=hello,
        op_kwargs={'age': '22'}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task2 >> task1