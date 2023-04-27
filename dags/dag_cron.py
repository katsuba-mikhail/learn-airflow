import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'learner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id='dag_cron_v3',
    start_date=datetime(2023, 4, 20),
    schedule_interval='59 12 * APR THU',
)

def dag_cron():
    
    @task()
    def use_cron():
        print('hello cron!')
    
    use_cron()

run_dag = dag_cron()