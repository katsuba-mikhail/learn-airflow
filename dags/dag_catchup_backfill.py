from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'learner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_catcup_backfill_v2',
    default_args=default_args,
    start_date=datetime(2023, 4, 12),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo in terminal use backfill command: '
        + 'airflow dags backfill -s 2023-04-10 -e 2023-04-20 dag_catcup_backfill_v2'
    )

task1