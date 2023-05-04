from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'tester',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='dag_test_today_catchup_v1',
    description='Test how work function datetime.today() in catchup mode.',
    default_args=default_args,
    start_date=datetime(2023, 5, 1),
    schedule_interval='@daily',
    catchup=True,
)

def main():

    @task
    def print_today():
        date_today = datetime.today()
        print(f'today: {date_today}')
        print("""
            When running tasks retroactively, 
            the actual time when the task was started is used, 
            not the time retroactively.
        """)

    print_today()

run = main()

