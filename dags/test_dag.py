from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="test_aws_s3",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    list_s3_files = S3ListOperator(
        task_id="list_s3_buckets",
        bucket="e2e-shop-bucket", 
        aws_conn_id="aws_default",
    )
