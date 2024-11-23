import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3

def run_glue_job(**kwargs):
    glue_client = boto3.client('glue', region_name='eu-west-3')

    response = glue_client.start_job_run(
        JobName='glue_transform'
    )
    job_run_id = response['JobRunId']
    print(f"Started Glue Job with Run ID: {job_run_id}")

    while True:
        status_response = glue_client.get_job_run(
            JobName='glue_transform',
            RunId=job_run_id
        )
        state = status_response['JobRun']['JobRunState']
        print(f"Current Glue Job State: {state}")
        if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        time.sleep(30)

    if state != 'SUCCEEDED':
        raise Exception(f"Glue Job failed with state: {state}")

    print("Glue Job completed successfully.")

with DAG(
    dag_id="test_aws_glue",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["aws", "glue"],
) as dag:

    glue_task = PythonOperator(
        task_id="test_glue_job",
        python_callable=run_glue_job,
        provide_context=True,  # Habilita el paso de contexto si necesitas kwargs
    )