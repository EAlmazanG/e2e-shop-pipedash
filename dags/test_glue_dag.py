import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import aws_setup

def run_glue_job(**kwargs):
    glue_client = boto3.client(
        'glue',
        region_name=aws_setup.conf['region_name'],
        aws_access_key_id=aws_setup.conf['aws_access_key_id'],
        aws_secret_access_key=aws_setup.conf['aws_secret_access_key']
    )

    response = glue_client.start_job_run(
        JobName='glue_transform'
    )
    job_run_id = response['JobRunId']
    print(f"Started Glue Job with Run ID: {job_run_id}")

  
    while True:
        try:
            status_response = glue_client.get_job_run(
                JobName='glue_transform',
                RunId=job_run_id
            )
            state = status_response['JobRun']['JobRunState']
            print(f"Current Glue Job State: {state}")
            
            if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                break

            time.sleep(30)

        except Exception as e:
            print(f"Error while monitoring Glue Job: {e}")
            raise

with DAG(
    dag_id="test_aws_glue",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["aws", "glue","test","e2e-shop"],
    dagrun_timeout=timedelta(hours=1)
) as dag:

    glue_task = PythonOperator(
        task_id="test_glue_job",
        python_callable=run_glue_job,
        provide_context=True,
    )