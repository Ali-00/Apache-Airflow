from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'github_crawler',
    default_args=default_args,
    description='Crawl Github API and store data in Google Cloud Storage',
    schedule_interval=timedelta(hours=1),
)


p = os.path.dirname(__file__)
dag_folder = os.path.join(*os.path.split(p)[:-1])
print("---------------------------------------------------  DAG PATH", dag_folder)
file_path = os.path.join(dag_folder, '/spark/app/crawler_main.py')
print("---------------------------------------------------   FILE ", file_path)

github_crawler = BashOperator(
    task_id='github_crawler',
    bash_command='python '+file_path,
    dag=dag, 
)

# D:/ml_sense/Apache_Airflow/spark/app/crawler_main.py


