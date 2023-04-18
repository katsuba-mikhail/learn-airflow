from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_arg = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='first_dag_v4',
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

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo task2 run after task 1'
    )

    task3 = BashOperator(
        task_id='thrird_task',
        bash_command='echo task3 run with task2 after task1'
    )

#  task1 >> task2 = task1.set_downstream(task2)
#  task1 >> task3 = task1.set_downstream(task3)

    task1 >> task2
    task1 >> task3