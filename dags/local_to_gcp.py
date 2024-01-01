from datetime import datetime
import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import logging
import pandas as pd
import json


# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="gcpupload", start_date=airflow.utils.dates.days_ago(1), schedule_interval="10 2 * * *") as dag:

    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        a = 1
        b = 2
        c = a + b
        logging.info("the sum of a and b is {}".format(c)) # Print statements won't show in dag logs. Use this instead
        # Access Dag Logs by clicking on the dag => graph view => click whataver task you want => logs
        return "airflow"
    
    @task()
    def test():
        with open('/run/secrets/credentials.json') as file:    
            data = json.load(file)
            logging.info(data)


    # @task()
    # def upload_file_to_gcs():
    #     hook = GCSHook(gcp_conn_id="djfnalkalkfjlka")
    #     hook.upload(bucket_name="github_crawler_muzamal", object_name="student.csv", filename="/opt/airflow/files/student.csv", mime_type="text/csv")
    #     logging.info("uploading {}".format("file to gcs"))
    #     return "Uploaded to Gcp"
    
    @task()
    def upload_dataframe_to_gcs():
        # create a new pandas DataFrame
        data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Age': [25, 30, 35]}
        df = pd.DataFrame(data)
        
        # write the DataFrame to a CSV file in memory
        csv_file = df.to_csv(index=False)
        
        # create a GCSHook and upload the CSV file to Google Cloud Storage
        hook = GCSHook(gcp_conn_id="test_connection")
        hook.upload(bucket_name="github_crawler_muzamal", object_name="dummy.csv", data=csv_file, mime_type="text/csv")
        
        logging.info("Uploaded new DataFrame as CSV to GCS")
        return "Uploaded dataframe to GCP"

    # Set dependencies between tasks
    hello >> airflow() >> test() >> upload_dataframe_to_gcs()