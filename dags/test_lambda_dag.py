from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator

with DAG(
    dag_id="test_aws_lambda",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["aws", "lambda"],
) as dag:

    lambda_task = AwsLambdaInvokeFunctionOperator(
        task_id="test_lambda_function",
        function_name="lambda-e2eShop-upload-product-descriptions",
        payload={},
        log_type="Tail",
        aws_conn_id="aws_default",
    )
