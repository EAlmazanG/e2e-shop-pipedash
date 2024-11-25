from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import time
from datetime import timedelta
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

    response = glue_client.start_job_run(JobName='glue_transform')
    job_run_id = response['JobRunId']
    print(f"Started Glue Job with Run ID: {job_run_id}")

    while True:
        try:
            status_response = glue_client.get_job_run(JobName='glue_transform', RunId=job_run_id)
            state = status_response['JobRun']['JobRunState']
            print(f"Current Glue Job State: {state}")
            
            if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                break

            time.sleep(30)

        except Exception as e:
            print(f"Error while monitoring Glue Job: {e}")
            raise

    if state != 'SUCCEEDED':
        raise Exception(f"Glue Job failed with state: {state}")

    print("Glue Job completed successfully.")

# DAG definition
with DAG(
    dag_id="e2e-shop-complete-pipeline",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["aws", "lambda", "glue", "snowflake", "e2e-shop"],
    dagrun_timeout=timedelta(hours=2),
) as dag:

    # Step 1: Lambda Function to extract data and upload to S3
    lambda_task = LambdaInvokeFunctionOperator(
        task_id="extract-invoke-lambda",
        function_name="lambda-e2eShop-upload-product-descriptions",
        payload="{}",  # Empty payload if not needed
        log_type="Tail",
        aws_conn_id="aws_default",
    )

    # Step 2: Glue Job to transform data
    glue_task = PythonOperator(
        task_id="transform-run-glue-job",
        python_callable=run_glue_job,
        provide_context=True,
    )

    # Define task dependencies
    lambda_task >> glue_task
