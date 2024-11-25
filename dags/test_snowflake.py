from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# DAG Config
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

IMPORT_TABLES = {
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
    "dim_customers": """
        COPY INTO dim_customers
        FROM (
            SELECT
                $1:customer_id::INT AS customer_id,
                $1:customer_name::STRING AS customer_name
            FROM @aws_s3_stage/dim_customers/
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
}

GENERATE_ETLS = {
    "etl_items": """
        create or replace table etl_items as
        select * from int_items;
    """
}

# DAG Definition
with DAG(
    dag_id="test_snowflake",
    default_args=default_args,
    description="ETL Pipeline from S3 to Snowflake",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["snowflake","test","e2e-shop"],
) as dag:

    import_tasks = []
    for table, query in IMPORT_TABLES.items():
        task = SnowflakeOperator(
            task_id=f"import_{table}",
            snowflake_conn_id="snowflake_default",
            sql=query,
        )
        import_tasks.append(task)

    generate_int_tables_tasks = []
    for table, query in GENERATE_INT_TABLES.items():
        task = SnowflakeOperator(
            task_id=f"generate_{table}",
            snowflake_conn_id="snowflake_default",
            sql=query,
        )
        generate_int_tables_tasks.append(task)

    generate_etl_tasks = []
    for table, query in GENERATE_ETLS.items():
        task = SnowflakeOperator(
            task_id=f"generate_{table}",
            snowflake_conn_id="snowflake_default",
            sql=query,
        )
        generate_etl_tasks.append(task)

    # Set dependencies
    for import_task in import_tasks:
        for generate_int_task in generate_int_tables_tasks:
            import_task >> generate_int_task

    for generate_int_task in generate_int_tables_tasks:
        for generate_etl_task in generate_etl_tasks:
            generate_int_task >> generate_etl_task