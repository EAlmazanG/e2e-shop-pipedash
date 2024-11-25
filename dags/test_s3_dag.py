from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

def print_s3_files(file_list):
    print("Check bucket files:")
    for file in file_list:
        print(file)

with DAG(
    dag_id="test_aws_s3",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags = ["aws","test","e2e-shop"]
) as dag:
    list_s3_files = S3ListOperator(
        task_id="list_s3_buckets",
        bucket="e2e-shop-bucket",
        aws_conn_id="aws_default",
    )

    log_files = PythonOperator(
        task_id="log_s3_files",
        python_callable=print_s3_files,
        op_args=[list_s3_files.output],
    )

    list_s3_files >> log_files
