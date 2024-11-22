from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def test_task():
    print("Hello World!")

with DAG(
    dag_id="test_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="test_task",
        python_callable=test_task,
    )
