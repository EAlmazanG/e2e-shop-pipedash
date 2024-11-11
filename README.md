# e2eShop-pipeDash

<!--
![Preview of the dashboard]()

<br></br>
<p align="center">
    <a href= target="_blank">
        <img src="https://img.shields.io/badge/Try%20the%20Live%20Demo!-4CAF50?style=for-the-badge&logo=streamlit&logoColor=white" alt="Live Demo">
    </a>
</p>
<br></br>
-->

## Overview
**e2eShop-pipeDash** is a complete end-to-end pipeline and dashboard project designed to provide insights into an eCommerce business. It gathers, processes, and visualizes key metrics such as sales, inventory levels, and customer behavior, enabling data-driven decisions for eCommerce optimization. This project demonstrates integration with AWS (for data ingestion and storage in S3), Airflow (for orchestration), Snowflake (as a data warehouse), and Tableau for final data visualization.

## Problem Statement
Many eCommerce businesses need a streamlined, scalable way to monitor and analyze their data. This project aims to solve this problem by building a pipeline that captures essential metrics, organizes data for efficient querying, and presents the results in a dashboard for quick insights. This approach provides business leaders with a clear, actionable view of their performance, inventory, and customer behavior.

## Technologies
- **AWS Lambda**: Automates data ingestion from API or source files to S3 (serving as a datalake).
- **Amazon S3**: Storage for raw data files.
- **Apache Airflow**: Manages ETL processes, transforming data in Snowflake.
- **Snowflake**: Data warehouse for structured storage and querying.
- **Tableau**: Dashboard for visualizing the final metrics and insights.

## Folder Structure

```bash
e2eShop-pipeDash/
│
├── data/                       # Sample data for local testing
│   ├── sample_data.csv         # Example file for simulating data ingestion
│
├── dags/                       # Airflow DAGs for ETL process
│   ├── ecommerce_etl.py        # DAG defining the ingestion, transformation, and loading process
│
├── src/                        # Core scripts for ingestion and transformation
│   ├── lambda_ingest.py        # AWS Lambda script for data ingestion
│   ├── transformations.py      # Functions for data cleaning and transformation in Airflow
│   ├── snowflake_queries.sql   # SQL scripts for data processing in Snowflake
│
├── config/                     # Configuration files for services
│   ├── config_s3.py            # S3 configuration for data storage
│   ├── config_snowflake.py     # Snowflake connection settings
│   ├── config_lambda.json      # Lambda configuration details
│   ├── airflow.cfg             # Airflow configuration
│
├── dashboard/                  # Tableau dashboard files
│   └── ecommerce_dashboard.twbx # Tableau file for visualizing metrics
│
├── notebooks/                  # Notebooks for prototyping and data exploration
│   ├── data_exploration.ipynb  # Notebook for initial data exploration
│   ├── etl_prototype.ipynb     # Prototype of ETL process and testing
│
├── README.md                   # Documentation and project overview
├── requirements.txt            # Dependencies
├── environment.yml             # Conda environment configuration (optional)
└── .gitignore                  # Excludes unnecessary files from version control
```

## Project Workflow

### Data Ingestion
AWS Lambda ingests data from the eCommerce source (e.g., transactions, inventory data) and uploads it to an Amazon S3 bucket, serving as a datalake. This initial ingestion is configured to run automatically based on scheduled intervals or data availability.

### Data Transformation
Once the data is in S3, Airflow orchestrates the ETL process. The Airflow DAG (`ecommerce_etl.py`) reads data from S3, processes it within Snowflake using SQL transformations, and organizes it into staging and final tables, ready for analysis and reporting.

### Data Storage and Management
Snowflake serves as the data warehouse, storing clean, transformed data. The tables are structured to support quick querying and analysis, with partitions and indexes optimized for eCommerce metrics such as sales trends, inventory management, and customer segmentation.

### Data Visualization
Tableau connects to Snowflake to visualize the final dataset. 
