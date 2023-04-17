from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_arg = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='first_dag',
    default_args=default_arg,
    description='First dag for learning',
    start_date=datetime(2023, 4, 12),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world, from first dag'
    )