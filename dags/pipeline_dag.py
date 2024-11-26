from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import time
from datetime import timedelta
import boto3
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import aws_setup


IMPORT_TABLES = {
    "dim_customers": """
        COPY INTO dim_customers
        FROM (
            SELECT
                $1:customer_id::INT AS customer_id,
                $1:customer_name::STRING AS customer_name
            FROM @aws_s3_stage/dim_customers/
        )
        FILE_FORMAT = (TYPE = PARQUET)
    """,
    "dim_date": """
        COPY INTO dim_date
        FROM (
            SELECT
                $1:date::DATE AS date,
                $1:date_id::INT AS date_id,
                $1:year::INT AS year,
                $1:month::INT AS month,
                $1:day::INT AS day,
                $1:day_of_week::STRING AS day_of_week,
                $1:is_weekend::BOOLEAN AS is_weekend
            FROM @aws_s3_stage/dim_date/
        )
        FILE_FORMAT = (TYPE = PARQUET)
    """,
    "dim_item_family": """
        COPY INTO dim_item_family
        FROM (
            SELECT
                $1:item_family_id::INT AS item_family_id,
                $1:item_family_description::STRING AS item_family_description,
                $1:item_category::STRING AS item_category
            FROM @aws_s3_stage/dim_item_family/
        )
        FILE_FORMAT = (TYPE = PARQUET)
    """,
    "dim_items": """
        COPY INTO dim_items
        FROM (
            SELECT
                $1:item_uuid::STRING AS item_uuid,
                $1:item_id::STRING AS item_id,
                $1:item_family_id::INT AS item_family_id,
                $1:item_description::STRING AS item_description,
                $1:item_variant::STRING AS item_variant,
                $1:is_operational_item::BOOLEAN AS is_operational_item,
                $1:is_unknown_item::BOOLEAN AS is_unknown_item,
                $1:is_special_item::BOOLEAN AS is_special_item,
                $1:is_modification_item::BOOLEAN AS is_modification_item,
                $1:is_error_item::BOOLEAN AS is_error_item
            FROM @aws_s3_stage/dim_items/
        )
        FILE_FORMAT = (TYPE = PARQUET)
    """,
    "dim_location": """
        COPY INTO dim_location
        FROM (
            SELECT
                $1:country_id::INT AS country_id,
                $1:iso_country_code::STRING AS iso_country_code,
                $1:country_name::STRING AS country_name,
                $1:country_code_name::STRING AS country_code_name
            FROM @aws_s3_stage/dim_location/
        )
        FILE_FORMAT = (TYPE = PARQUET)
    """,
    "fact_transactions": """
        COPY INTO fact_transactions
        FROM (
            SELECT
                $1:transaction_id::BIGINT AS transaction_id,
                $1:invoice_id::STRING AS invoice_id,
                $1:event_timestamp_invoiced_at::TIMESTAMP AS event_timestamp_invoiced_at,
                $1:date_id::INT AS date_id,
                $1:item_uuid::STRING AS item_uuid,
                $1:item_id::STRING AS item_id,
                $1:quantity_amount::INT AS quantity_amount,
                $1:unit_price_eur::FLOAT AS unit_price_eur,
                $1:total_price_eur::FLOAT AS total_price_eur,
                $1:customer_id::INT AS customer_id,
                $1:country_id::INT AS country_id
            FROM @aws_s3_stage/fact_transactions/
        )
        FILE_FORMAT = (TYPE = PARQUET)
    """
}

GENERATE_INT_TABLES = {
    "int_items": """
        create or replace table int_items as
        select 
            ft.item_uuid,
            di.item_description,
            di.item_family_id,
            df.item_family_description,
            df.item_category,
            di.is_operational_item,
            di.is_unknown_item,
            di.is_special_item,
            di.is_modification_item,
            di.is_error_item,
            round(sum(ft.total_price_eur), 2) as total_sales_amount,
            count(distinct ft.customer_id) as total_customers,
            sum(ft.quantity_amount) as total_quantity,
            count(distinct ft.transaction_id) as total_transactions
        from 
            fact_transactions as ft
            inner join dim_items as di on 
                ft.item_uuid = di.item_uuid
            inner join dim_item_family as df on 
                di.item_family_id = df.item_family_id
        group by 
            ft.item_uuid, 
            di.item_description, 
            di.item_family_id, 
            df.item_family_description, 
            df.item_category, 
            di.is_operational_item, 
            di.is_unknown_item, 
            di.is_special_item, 
            di.is_modification_item, 
            di.is_error_item;
    """,
    "int_transactions": """
        create or replace table int_transactions as
        select 
            ft.transaction_id,
            ft.invoice_id,
            ft.item_uuid,
            di.item_id,
            di.item_description,
            df.item_family_description,
            df.item_category,
            di.item_variant,
            di.is_operational_item,
            di.is_unknown_item,
            di.is_special_item,
            di.is_modification_item,
            di.is_error_item,
            round(ft.unit_price_eur, 2) as unit_price_eur,
            round(ft.total_price_eur, 2) as total_price_eur,
            ft.quantity_amount,
            ft.customer_id,
            dl.country_id,
            dl.country_name,
            dd.date_id,
            dd.date,
            dd.day_of_week,
            case 
                when di.is_operational_item = false 
                     and di.is_unknown_item = false 
                     and di.is_special_item = false 
                     and di.is_modification_item = false 
                     and di.is_error_item = false 
                     and (ft.quantity_amount < 0 or ft.total_price_eur < 0) 
                then true 
                else false 
            end as is_return
        from 
            fact_transactions as ft
            left join dim_items as di on 
                ft.item_uuid = di.item_uuid
            left join dim_item_family as df on 
                di.item_family_id = df.item_family_id
            left join dim_location as dl on 
                ft.country_id = dl.country_id
            left join dim_date as dd on 
                ft.date_id = dd.date_id;
    """,
    "int_customers":"""
        create or replace table int_customers as
            with 
                favorite_category_per_customer as (
                    select 
                        customer_id,
                        item_category,
                        count(*) as category_count,
                        row_number() over (
                            partition by customer_id 
                            order by count(*) desc
                        ) as category_rank
                    from int_transactions
                    group by 
                        customer_id, 
                        item_category
                )
            
            select 
                it.customer_id,
                dl.country_name,
                round(sum(it.total_price_eur), 2) as total_sales_amount,
                sum(it.quantity_amount) as total_quantity,
                count(distinct it.transaction_id) as total_transactions,
                count(distinct it.invoice_id) as total_invoices,
                sum(case when it.is_return then 1 else 0 end) as total_returns,
                fc.item_category as favorite_category,
                round(sum(it.total_price_eur) / nullif(count(distinct it.transaction_id), 0),2) as avg_sales_per_transaction
                from 
                    int_transactions as it 
                    left join dim_location as dl on 
                        it.country_id = dl.country_id
                    left join favorite_category_per_customer as fc on 
                        it.customer_id = fc.customer_id and fc.category_rank = 1
            group by 
                it.customer_id, 
                dl.country_name, 
                fc.item_category;
    """,
    "int_invoices":"""
        create or replace table int_invoices as
        select 
            it.invoice_id,
            it.date_id,
            dd.date,
            it.customer_id,
            dl.country_name,
            dd.day_of_week,
            dl.country_id,
            round(sum(it.total_price_eur), 2) as total_invoice_amount,
            sum(it.quantity_amount) as total_quantity,
            count(distinct it.item_uuid) as total_items,
            max(it.is_return) as has_return,
            max(it.is_operational_item) as has_operational_item,
            max(it.is_unknown_item) as has_unknown_item,
            max(it.is_special_item) as has_special_item,
            max(it.is_modification_item) as has_modification_item,
            max(it.is_error_item) as has_error_item
        from 
            int_transactions as it 
            left join dim_date as dd on 
                it.date_id = dd.date_id
            left join dim_location as dl on
                it.country_id = dl.country_id
        group by 
            it.invoice_id,
            it.date_id, 
            dd.date, 
            it.customer_id, 
            dl.country_name, 
            dd.day_of_week, 
            dl.country_id
    """
}

GENERATE_ETLS = {
    "etl_items": """
        create or replace table etl_items as
        select * from int_items;
    """,
    "etl_transactions": """
        create or replace table etl_transactions as
        select * from int_transactions;
    """,
    "etl_customers": """
        create or replace table etl_customers as
        select * from int_customers;
    """,
    "etl_invoices": """
        create or replace table etl_invoices as
        select * from int_invoices;
    """
}

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

    # Step 3: Import transformed data from S3 to snowflake
    import_tasks = {
        table: SnowflakeOperator(
            task_id=f"import_{table}",
            snowflake_conn_id="snowflake_default",
            sql=query,
        )
        for table, query in IMPORT_TABLES.items()
    }

    # Step 4: Generate intermediate tables in snowflake
    generate_int_tables_tasks = {
        table: SnowflakeOperator(
            task_id=f"generate_{table}",
            snowflake_conn_id="snowflake_default",
            sql=query,
        )
        for table, query in GENERATE_INT_TABLES.items()
    }

    # Step 5: Generate etl in snowflake
    generate_etl_tasks = {
        table: SnowflakeOperator(
            task_id=f"generate_{table}",
            snowflake_conn_id="snowflake_default",
            sql=query,
        )
        for table, query in GENERATE_ETLS.items()
    }

    # Set task dependencies
    lambda_task >> glue_task

    for import_task in import_tasks.values():
        glue_task >> import_task

    import_tasks["dim_items"] >> generate_int_tables_tasks["int_items"]
    import_tasks["dim_item_family"] >> generate_int_tables_tasks["int_items"]
    import_tasks["fact_transactions"] >> generate_int_tables_tasks["int_items"]

    import_tasks["dim_date"] >> generate_int_tables_tasks["int_transactions"]
    import_tasks["dim_items"] >> generate_int_tables_tasks["int_transactions"]
    import_tasks["dim_item_family"] >> generate_int_tables_tasks["int_transactions"]
    import_tasks["dim_location"] >> generate_int_tables_tasks["int_transactions"]
    import_tasks["fact_transactions"] >> generate_int_tables_tasks["int_transactions"]

    import_tasks["dim_location"] >> generate_int_tables_tasks["int_invoices"]
    import_tasks["dim_date"] >> generate_int_tables_tasks["int_invoices"]
    generate_int_tables_tasks["int_transactions"] >> generate_int_tables_tasks["int_invoices"]

    import_tasks["dim_location"] >> generate_int_tables_tasks["int_customers"]
    generate_int_tables_tasks["int_transactions"] >> generate_int_tables_tasks["int_customers"]

    generate_int_tables_tasks["int_items"] >> generate_etl_tasks["etl_items"]
    generate_int_tables_tasks["int_transactions"] >> generate_etl_tasks["etl_transactions"]
    generate_int_tables_tasks["int_invoices"] >> generate_etl_tasks["etl_invoices"]
    generate_int_tables_tasks["int_customers"] >> generate_etl_tasks["etl_customers"]