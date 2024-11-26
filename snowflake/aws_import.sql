-- Set integration and COPY TO tables from AWS S3, for continuos update use SNOWPIPE instead of COPY YO

-- set database and schema to use

USE DATABASE E2E_SHOP_PIPEDASH
;
USE SCHEMA BI
;

-- set integration

CREATE OR REPLACE STORAGE INTEGRATION aws_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::***:role/snowflake-e2e-shop'
STORAGE_ALLOWED_LOCATIONS = ('s3://e2e-shop-bucket')
STORAGE_AWS_EXTERNAL_ID = '***'
;

-- describe integration

DESC INTEGRATION aws_s3_integration
;

-- create stage

CREATE OR REPLACE STAGE aws_s3_stage
URL = 's3://e2e-shop-bucket/clean'
STORAGE_INTEGRATION = aws_s3_integration
;

-- list stage
LIST @aws_s3_stage
;

-- dim_customers

CREATE OR REPLACE TABLE dim_customers (
    customer_id INT,
    customer_name STRING
)
;

COPY INTO dim_customers
FROM (
    SELECT
        $1:customer_id::INT AS customer_id,
        $1:customer_name::STRING AS customer_name
    FROM @aws_s3_stage/dim_customers/
)
FILE_FORMAT = (TYPE = PARQUET)
;
select * from dim_customers limit 20
;

-- dim_date

CREATE OR REPLACE TABLE dim_date (
    date DATE,
    date_id INT,
    year INT,
    month INT,
    day INT,
    day_of_week STRING,
    is_weekend BOOLEAN
);

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
;
select * from dim_date limit 20
;

-- dim_item_family

CREATE OR REPLACE TABLE dim_item_family (
    item_family_id INT,
    item_family_description STRING,
    item_category STRING
);

COPY INTO dim_item_family
FROM (
    SELECT
        $1:item_family_id::INT AS item_family_id,
        $1:item_family_description::STRING AS item_family_description,
        $1:item_category::STRING AS item_category
    FROM @aws_s3_stage/dim_item_family/
)
FILE_FORMAT = (TYPE = PARQUET)
;
select * from dim_item_family limit 20
;

-- dim_items

CREATE OR REPLACE TABLE dim_items (
    item_uuid STRING,
    item_id STRING,
    item_family_id INT,
    item_description STRING,
    item_variant STRING,
    is_operational_item BOOLEAN,
    is_unknown_item BOOLEAN,
    is_special_item BOOLEAN,
    is_modification_item BOOLEAN,
    is_error_item BOOLEAN
);

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
;
select * from dim_items limit 20
;

-- Tabla: dim_location

CREATE OR REPLACE TABLE dim_location (
    country_id INT,
    iso_country_code STRING,
    country_name STRING,
    country_code_name STRING
);

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
;
select * from dim_location limit 20
;

-- Tabla: fact_transactions

CREATE OR REPLACE TABLE fact_transactions (
    transaction_id BIGINT,
    invoice_id STRING,
    event_timestamp_invoiced_at TIMESTAMP,
    date_id INT,
    item_uuid STRING,
    item_id STRING,
    quantity_amount INT,
    unit_price_eur FLOAT,
    total_price_eur FLOAT,
    customer_id INT,
    country_id INT
);

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
;
select * from fact_transactions;
