# End-to-End Cloud Data Pipeline — Telecom Customer Churn

A fully automated, cloud-native data engineering pipeline built on AWS to ingest, clean, transform, and store telecom customer churn data into a star schema data warehouse, ready for BI dashboard consumption.



## Project Overview

This pipeline processes the IBM Telco Customer Churn dataset (7,043 records) through a series of AWS services — from raw CSV files in S3 all the way to a structured Amazon Redshift data warehouse — orchestrated weekly by Apache Airflow.

**Use Case:** Enable a BI dashboard to answer business questions such as:
- Which customer segments are most likely to churn?
- What are the top reasons customers are leaving?
- How much monthly revenue is lost due to churn?



## Architecture

```
Amazon S3 (Data Lake)
      │
      ▼
AWS Glue Crawler ──► AWS Glue Data Catalog ──► Amazon Athena (ad-hoc validation)
      │
      ▼
AWS Glue ETL Job (PySpark)
  - Deduplication
  - Missing value handling
  - Data type standardisation
  - Star schema split
      │
      ▼
Amazon Redshift (Data Warehouse)
  ├── dim_customer
  ├── dim_services
  ├── dim_contract
  └── fact_churn
      │
      ▼
BI Dashboard (e.g. Amazon QuickSight / Power BI)


All ETL steps orchestrated by Apache Airflow on Amazon EC2
```



## Tech Stack

| Layer | Technology |
|---|---|
| Data Lake | Amazon S3 |
| Schema Inference | AWS Glue Crawler |
| Metadata Store | AWS Glue Data Catalog |
| Ad-hoc Validation | Amazon Athena |
| Distributed Processing | AWS Glue ETL Job (Apache Spark / PySpark) |
| Data Warehouse | Amazon Redshift |
| Orchestration | Apache Airflow (on Amazon EC2) |
| Language | Python, SQL |



## Repository Structure

```
├── dags/
│   └── customer_churn_dag.py       # Airflow DAG — weekly pipeline orchestration
├── glue/
│   └── glue_etl_job.py             # PySpark ETL script — cleanse, transform, load
├── sql/
│   └── redshift_schema.sql         # Redshift star schema DDL
└── README.md
```



## Pipeline Components

### 1. Data Lake — Amazon S3
- The IBM Telco Customer Churn dataset is split into 3 CSV files to simulate real-world batch ingestion
- Files are stored in an S3 bucket acting as the raw data lake

### 2. Ingestion — AWS Glue Crawler
- Crawls the S3 bucket and automatically infers the schema of all CSV files
- Registers a unified table in the AWS Glue Data Catalog

### 3. Validation — Amazon Athena
- Used for ad-hoc SQL queries directly on S3 data to verify row counts and data quality before ETL

### 4. ETL — AWS Glue Job (PySpark)
The ETL job applies the following cleansing and transformation steps:

| Step | Description |
|---|---|
| Deduplication | `dropDuplicates()` on CustomerID |
| Missing value handling | Null `total_charges` filled with `monthly_charges`; null `churn_reason` filled with `"Unknown"`; rows with null `customerid`, `monthly_charges`, or `churn_label` dropped |
| Data type conversion | `zip_code`, `tenure_months`, `churn_value`, `churn_score` cast from long to int |
| Star schema split | Dataset split into 4 logical tables before loading into Redshift |

### 5. Data Warehouse — Amazon Redshift (Star Schema)

| Table | Contents |
|---|---|
| `dim_customer` | CustomerID, City, Zip Code, Gender, Senior Citizen, Partner, Dependents |
| `dim_services` | Phone, Internet, Streaming, Security, Backup, Device Protection, Tech Support |
| `dim_contract` | Contract type, Paperless Billing, Payment Method |
| `fact_churn` | Tenure, Monthly Charges, Total Charges, Churn Label, Churn Value, Churn Score, Churn Reason |

### 6. Orchestration — Apache Airflow

The DAG (`my_dag`) runs weekly with 3 sequential tasks:

```
tsk_glue_job_trigger
        │
        ▼
tsk_grab_glue_job_run_id
        │
        ▼
tsk_is_glue_job_finish_running (GlueJobSensor)
```



## Dataset

**IBM Telco Customer Churn** — available on [Kaggle](https://www.kaggle.com/datasets/yeanzc/telco-customer-churn-ibm-dataset)

- 7,043 rows, 33 columns
- Split into 3 CSV files for batch ingestion simulation
- Not included in this repository (download from Kaggle)


## Setup Notes

> This project was built and run on AWS. The infrastructure has since been terminated. The code files in this repository represent the full implementation.

To reproduce this pipeline you would need:
- An AWS account with access to S3, Glue, Redshift, Athena, and EC2
- An EC2 instance with Apache Airflow installed
- The Kaggle dataset uploaded to an S3 bucket
- AWS credentials configured in Airflow connections (`aws_s3_conn`)
