from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'learner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='dag_catcup_backfill_v1',
    default_args=default_args,
    start_date=datetime(2023, 4, 12),
    schedule_interval='@daily',
    catchup=True,
)

def dag1():

    @task()
    def task1():
        print('Catchup test')

new_dag = dag1()