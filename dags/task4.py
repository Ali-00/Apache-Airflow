from datetime import datetime
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

def scrape_data():
    repo_data = requests.get('https://jsonplaceholder.typicode.com/todos')
    repo_data = repo_data.json()
    users_details = []      
    for repo in repo_data:
        user_id = repo['userId']
        id = repo['id']
        title = repo['title']
        completed = repo['completed']
        users_details.append((id, user_id, title, completed))
    return users_details

def save_to_database():
    data = scrape_data()
    df = pd.DataFrame(data)
    engine = create_engine('postgresql://muzamal:abcd1234@cloudsql:5433/muzamal_db')
    df.to_sql('test_table2', engine, if_exists='append', index=False)

dag = DAG(
    'scrape_and_save_data',
    description='DAG to scrape data from API and save to cloud database',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

scrape_task = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_data,
    dag=dag
)

save_task = PythonOperator(
    task_id='save_to_database',
    python_callable=save_to_database,
    dag=dag
)

scrape_task >> save_task
