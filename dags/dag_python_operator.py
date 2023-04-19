from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'learner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def hello():
    print('hello world!')

with DAG(
    default_args=default_args,
    dag_id='dag_python_operator_v1',
    start_date=datetime(2023, 4, 12),
    schedule_interval='@daily',

) as dag:

    task1 = PythonOperator(
        task_id='hello',
        python_callable=hello,
    )