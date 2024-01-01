from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'my_dag',
    start_date=datetime(2023, 4, 11),
    schedule_interval='@once'
)

# Define the task
def my_task():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        database=PG_DATABASE
    )

    # Create a cursor object
    cur = conn.cursor()

    # Execute a query
    cur.execute('SELECT * FROM my_table')

    # Fetch the results
    results = cur.fetchall()

    # Print the results
    for row in results:
        print(row)

    # Close the cursor and connection
    cur.close()
    conn.close()

# Define the operator
operator = PythonOperator(
    task_id='my_task',
    python_callable=my_task,
    dag=dag
)
