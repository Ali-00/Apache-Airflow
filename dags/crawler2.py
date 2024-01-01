# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# # from airflow.providers.google.cloud.operators.gcs import GoogleCloudStorageUploadOperator
# # from airflow.providers.google.cloud.operators.gcs import GCSUploadOperator
# from airflow.providers.google.cloud.hooks.gcs import GCSHook
# # from airflow.providers.google.cloud.hooks.gcs import GoogleCloudStorageHook
# # from airflow.providers.google.cloud.operators.gcs import GCSFileTransferOperator
# # from airflow.
# from datetime import datetime, timedelta
# import google
# from google.cloud import storage
# import requests
# import csv
# import pandas as pd
# import argparse
# import logging
# import os
# # from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# from airflow.models.variable import Variable


# BASE_PATH = Variable.get("BASE_PATH")
# BUCKET_NAME = 'github_crawler_muzamal'
# GOOGLE_CLOUD_CONN_ID = Variable.get("GOOGLE_CLOUD_CONN_ID")
# BIGQUERY_TABLE_NAME = "bs_disaster"
# GCS_OBJECT_NAME = "repo_data.csv"
# # DATA_PATH = f"{BASE_PATH}/data"
# DATA_PATH = '/opt/airflow/data/'
# # OUT_PATH = f"{DATA_PATH}/{GCS_OBJECT_NAME}"
# OUT_PATH = DATA_PATH + GCS_OBJECT_NAME



# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/ml_sense/Apache_Airflow/secrets/service_account_key.json'

# def logger(str):
#     print('*************************************************************')
#     print(str)
#     print('*************************************************************')

# def get_repo_data(**context):
#     try:
#         response = requests.get(context['url'])
#         # if response!=None:
#         #     print("True-------------------")
#         logger(response)
#         if response.status_code == 200:
#             context['ti'].xcom_push(key='repo_data', value=response.json())
#             # context['ti'].xcom_push(key='repository_count', value=30)
#             # context['ti'].xcom_push(key='max_commit_count', value=30)
#         else:
#             logging.error(f"Error {response.status_code} getting data from {context['url']}")
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Request exception while getting data from {context['url']}: {str(e)}")

# def extract_repo_details(**context):
#     repo_data = context['ti'].xcom_pull(key='repo_data', task_ids='get_repo_data')
#     print(repo_data)
#     repository_count = 30
#     repo_details = []      
#     for repo in repo_data:
#         if len(repo_details) == repository_count:
#             break
#         try:
#             repo_id = repo['id']
#             repo_name = repo['name']
#             repo_url = repo['html_url']
#             repo_fullname = repo['full_name']
#             is_private = repo['private']
#             repo_description = repo['description']
#             owner_username = repo['owner']['login']
#             owner_url = repo['owner']['html_url']
#             owner_type = repo['owner']['type']
#             owner_site_admin = repo['owner']['site_admin']
#             is_fork = repo['fork']
#             repo_details.append((repo_id, repo_name, repo_url, repo_fullname, is_private, repo_description,
#                                 owner_username, owner_url, owner_type, owner_site_admin, is_fork))
#         except requests.exceptions.RequestException as e:
#             logging.error(f"Request exception while getting data")
#     logger(repo_details)
#     df = pd.DataFrame(repo_details, columns=['repo_id', 'repo_name', 'repo_url','repo_fullname','is_private','repo_description','owner_username','owner_url','owner_type','owner_site_admin','is_fork'])
#     # OUT_PATH2 = OUT_PATH.replace('/', '\\')
#     df.to_csv("/opt/airflow/files/repo_data.csv", index=False)
#     context['ti'].xcom_push(key='repo_details', value=repo_details)


# def extract_commit_details(**context):
#     repo_data = context['ti'].xcom_pull(key='repo_data', task_ids='get_repo_data')
    
#     max_commit_count = 30
#     commit_data = []
#     for repo in repo_data:
#         commit_url = f"{repo['url']}/commits"
#         try:
#             response = requests.get(commit_url)
#             if response.status_code == 200:
#                 commits = response.json()[:max_commit_count]
#                 for commit in commits:
#                     commit_data.append([
#                         commit['sha'],
#                         commit['commit']['message'],
#                         commit['html_url'],
#                         commit['commit']['author']['name'],
#                         commit['author']['login'],
#                         commit['commit']['author']['email'],
#                         commit['author']['html_url'],
#                         commit['commit']['committer']['name'],
#                         commit['committer']['login'],
#                         commit['commit']['committer']['email'],
#                         commit['committer']['html_url'],
#                     ])
#             else:
#                 logging.error(f"Error {response.status_code}")
#         except requests.exceptions.RequestException as e:
#             logging.error(f"Request exception while getting data")
#     logger(commit_data)
#     df = pd.DataFrame(commit_data, columns=['commit_hash', 'commit_message', 'commit_url','author_name','author_username','author_email','author_url','committer_name','committere_username','committer_email','committer_url'])
#     logger(df)
#     GCS_OBJECT_NAME = "commit_data.csv"
#     OUT_PATH = f"{DATA_PATH}/{GCS_OBJECT_NAME}"
#     # OUT_PATH2 = OUT_PATH.replace('/', '\\')
#     df.to_csv("/opt/airflow/files/commit_data.csv", index=False)
#     context['ti'].xcom_push(key='commit_data', value=commit_data)


# # def write_repo_data(**context):
# #     repo_details = context['ti'].xcom_pull(key='repo_details', task_ids='extract_repo_details')
# #     df = pd.DataFrame(repo_details, columns=['repo_id', 'repo_name', 'repo_url','repo_fullname','is_private','repo_description','owner_username','owner_url','owner_type','owner_site_admin','is_fork'])
# #     client = storage.Client()
# #     export_bucket = client.get_bucket('github_crawler_muzamal')
# #     export_bucket.blob('repo.csv').upload_from_string(df.to_csv(),'text/csv')


# def write_repo_data(**context):
#     repo_details = context['ti'].xcom_pull(key='repo_details', task_ids='extract_repo_details')
#     logger(repo_details)
#     df = pd.DataFrame(repo_details, columns=['repo_id', 'repo_name', 'repo_url','repo_fullname','is_private','repo_description','owner_username','owner_url','owner_type','owner_site_admin','is_fork'])
#     OUT_PATH2 = OUT_PATH.replace('/', '\\')
#     hook = GCSHook(gcp_conn_id="djfnalkalkfjlka")
#     hook.upload(bucket_name="github_crawler_muzamal", object_name="repo_data.csv", filename="/opt/airflow/files/repo_data.csv", mime_type="text/csv")
#     logging.info("uploading {}".format("file to gcs"))
#     # df.to_csv(OUT_PATH2, index=False)
#     # logger(df)
#     # stored_data_gcs = LocalFilesystemToGCSOperator(
#     #     task_id="store_to_gcs",
#     #     gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
#     #     src=OUT_PATH2,
#     #     dst=GCS_OBJECT_NAME,
#     #     bucket=BUCKET_NAME
#     # )


# # ----------------------------------------------------------------------
#     # gcs_hook = GCSHook(gcp_conn_id='github_crawler')
#     # bucket_name = 'github_crawler_muzamal'
#     # object_name = 'repo.csv'

#     # p = os.path.dirname(__file__)
#     # dag_folder = os.path.join(*os.path.split(p)[:-1])
#     # print("---------------------------------------------------  DAG PATH", dag_folder)
#     # dag_folder = dag_folder+'/repo.csv'
#     # local_file_path = dag_folder

#     # gcs_hook.upload(bucket_name, object_name, local_file_path)


# #  -------------------------------------------------------------------------------------------------------



#     # GCSFileTransferOperator(
#     # task_id='upload_file',
#     # method='upload',
#     # bucket=bucket_name,
#     # object=object_name,
#     # filename=local_file_path,
#     # gcp_conn_id='github_crawler')


#     # ------------------------------------------------------------------------------------------------------
#     # file_contents = df.to_csv(index=False)
    
#     # upload_task = GoogleCloudStorageUploadOperator(
#     #     task_id='upload_file',
#     #     bucket=bucket_name,
#     #     object=object_name,
#     #     data=file_contents,
#     #     mime_type='text/csv',
#     #     google_cloud_storage_conn_id='google_cloud_default',
#     #     provide_context=True
#     # )
#     # ------------------------------------------------------------------------------------------------------


# def write_commit_data(**context):
#     commit_data = context['ti'].xcom_pull(key='commit_data', task_ids='extract_commit_details')
#     logger(commit_data)
#     df = pd.DataFrame(commit_data, columns=['commit_hash', 'commit_message', 'commit_url','author_name','author_username','author_email','author_url','committer_name','committere_username','committer_email','committer_url'])
#     logger(df)
#     GCS_OBJECT_NAME = "commit_data.csv"
#     # DATA_PATH = f"{BASE_PATH}/data"
#     # OUT_PATH = f"{DATA_PATH}/{GCS_OBJECT_NAME}"
#     OUT_PATH = DATA_PATH + GCS_OBJECT_NAME
#     OUT_PATH2 = OUT_PATH.replace('/', '\\')
#     # df.to_csv(OUT_PATH2, index=False)
#     hook = GCSHook(gcp_conn_id="djfnalkalkfjlka")
#     hook.upload(bucket_name="github_crawler_muzamal", object_name="commit_data.csv", filename="/opt/airflow/files/commit_data.csv", mime_type="text/csv")
#     logging.info("uploading {}".format("file to gcs"))



#     # stored_data_gcs = LocalFilesystemToGCSOperator(
#     #     task_id="store_to_gcs",
#     #     gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
#     #     src=OUT_PATH2,
#     #     dst=GCS_OBJECT_NAME,
#     #     bucket=BUCKET_NAME
#     # )
    

# # -----------------------------------------------------------------------------------------------------------

#     # stored_data_gcs = LocalFilesystemToGCSOperator(
#     #     task_id="store_to_gcs",
#     #     gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
#     #     src=OUT_PATH,
#     #     dst=GCS_OBJECT_NAME,
#     #     bucket=BUCKET_NAME
#     # )


# # ----------------------------------------------------------------------------------------------------------
# # gcs_hook = GCSHook(gcp_conn_id='github_crawler')
# #     bucket_name = 'github_crawler_muzamal'
# #     object_name = 'commit.csv'
# #     df.to_csv('commit.csv',index=False)

# #     p = os.path.dirname(__file__)
# #     dag_folder = os.path.join(*os.path.split(p)[:-1])
# #     print("---------------------------------------------------  DAG PATH", dag_folder)
# #     dag_folder = dag_folder+'/commit.csv'
# #     local_file_path = dag_folder

# #     gcs_hook.upload(bucket_name, object_name, local_file_path)
    

# #  ---------------------------------------------------------------------------------------------------------


#     # GCSFileTransferOperator(
#     # task_id='upload_file',
#     # method='upload',
#     # bucket=bucket_name,
#     # object=object_name,
#     # filename=local_file_path,
#     # gcp_conn_id='github_crawler')



#     # ------------------------------------------------------------------------------------------------------
#     # file_contents = df.to_csv('commit.csv',index=False)
#     # upload_task = GoogleCloudStorageUploadOperator(
#     #     task_id='upload_file',
#     #     bucket=bucket_name,
#     #     object=object_name,
#     #     data=file_contents,
#     #     mime_type='text/csv',
#     #     google_cloud_storage_conn_id='google_cloud_default',
#     #     provide_context=True
#     # )
#     # ------------------------------------------------------------------------------------------------------



# # def write_commit_data(**context):
# #     commit_data = context['ti'].xcom_pull(key='commit_data', task_ids='extract_commit_details')
# #     df1 = pd.DataFrame(commit_data, columns=['commit_hash', 'commit_message', 'commit_url','author_name','author_username','author_email','author_url','committer_name','committere_username','committer_email','committer_url'])
# #     client = storage.Client()
# #     export_bucket = client.get_bucket('github_crawler_muzamal')
# #     export_bucket.blob('commit.csv').upload_from_string(df1.to_csv(),'text/csv')


# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 4, 18),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1
# }

# dag = DAG('github_crawler2', default_args=default_args, description='Crawl Github API and store data in Google Cloud Storage',schedule_interval=timedelta(hours=1),)

# get_repo_data_task = PythonOperator(
#     task_id='get_repo_data',
#     python_callable=get_repo_data,
#     op_kwargs={'url': 'https://api.github.com/repositories?since=10000'},
#     dag=dag
# )

# extract_repo_details_task = PythonOperator(
#     task_id='extract_repo_details',
#     python_callable=extract_repo_details,
#     op_kwargs={'repo_data': '{{ ti.xcom_pull(task_ids="get_repo_data") }}'},
#     dag=dag
# )

# extract_commit_details_task = PythonOperator(
#     task_id='extract_commit_details',
#     python_callable=extract_commit_details,
#     op_kwargs={'repo_data': '{{ ti.xcom_pull(task_ids="get_repo_data") }}'},
#     dag=dag
# )

# write_repo_data_task = PythonOperator(
#     task_id='write_repo_data',
#     python_callable=write_repo_data,
#     op_kwargs={'repo_details': '{{ ti.xcom_pull(task_ids="extract_repo_details") }}'},
#     dag=dag
# )

# write_commit_data_task = PythonOperator(
#     task_id='write_commit_data',
#     python_callable=write_commit_data,
#     op_kwargs={'commit_data': '{{ ti.xcom_pull(task_ids="extract_commit_details") }}'},
#     dag=dag
# )

# # get_repo_data_task >> [extract_repo_details_task, extract_commit_details_task] >> [write_repo_data_task, write_commit_data_task]
# get_repo_data_task >> extract_repo_details_task
# get_repo_data_task >> extract_commit_details_task

# extract_repo_details_task >> write_repo_data_task
# extract_commit_details_task >> write_commit_data_task
