import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import col, trim, when
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info("Starting Glue Job: Reading from S3 Data Catalog")

# ---- 1. READ FROM S3 --------------------------------------------------------------------------------------------
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="customer-churn-s3-glue-database",
    table_name="data_engineering_cw_2507577",
    transformation_ctx="raw_dyf"
)

# ---- 2. APPLY SCHEMA MAPPING ----------------------------------------------------------------------------
mapped_dyf = ApplyMapping.apply(
    frame=raw_dyf,
    mappings=[
        ("customerid", "string", "customerid", "string"),
        ("city", "string", "city", "string"),
        ("zip code", "long", "zip_code", "int"),
        ("gender", "string", "gender", "string"),
        ("senior citizen", "string", "senior_citizen", "string"),
        ("partner", "string", "partner", "string"),
        ("dependents", "string", "dependents", "string"),
        ("tenure months", "long", "tenure_months", "int"),
        ("phone service", "string", "phone_service", "string"),
        ("multiple lines", "string", "multiple_lines", "string"),
        ("internet service", "string", "internet_service", "string"),
        ("online security", "string", "online_security", "string"),
        ("online backup", "string", "online_backup", "string"),
        ("device protection", "string", "device_protection", "string"),
        ("tech support", "string", "tech_support", "string"),
        ("streaming tv", "string", "streaming_tv", "string"),
        ("streaming movies", "string", "streaming_movies", "string"),
        ("contract", "string", "contract", "string"),
        ("paperless billing", "string", "paperless_billing", "string"),
        ("payment method", "string", "payment_method", "string"),
        ("monthly charges", "double", "monthly_charges", "double"),
        ("total charges", "double", "total_charges", "double"),
        ("churn label", "string", "churn_label", "string"),
        ("churn value", "long", "churn_value", "int"),
        ("churn score", "long", "churn_score", "int"),
        ("churn reason", "string", "churn_reason", "string")
    ],
    transformation_ctx="mapped_dyf"
)

# Convert to Spark DataFrame for cleansing
df = mapped_dyf.toDF()
logger.info(f"Row count before cleansing: {df.count()}")

# ---- 3. CLEANSING STEP 1 — DEDUPLICATION --------------------------------------------------
df = df.dropDuplicates(["customerid"])
logger.info(f"Row count after deduplication: {df.count()}")

# ---- 4. CLEANSING STEP 2 — MISSING VALUE HANDLING --------------------------------
# Drop rows where critical fields are null
df = df.filter(
    col("customerid").isNotNull() &
    col("monthly_charges").isNotNull() &
    col("churn_label").isNotNull()
)
# Fill missing total_charges with monthly_charges (new customers)
df = df.withColumn(
    "total_charges",
    when(col("total_charges").isNull(), col("monthly_charges"))
    .otherwise(col("total_charges"))
)
# Fill missing churn_reason with 'Unknown'
df = df.withColumn(
    "churn_reason",
    when(col("churn_reason").isNull(), "Unknown")
    .otherwise(col("churn_reason"))
)
logger.info(f"Row count after missing value handling: {df.count()}")

# ---- 5. SPLIT INTO DIMENSION AND FACT DATAFRAMES ------------------------------------
dim_customer_df = df.select(
    "customerid", "city", "zip_code", "gender",
    "senior_citizen", "partner", "dependents"
)

dim_services_df = df.select(
    "customerid", "phone_service", "multiple_lines", "internet_service",
    "online_security", "online_backup", "device_protection",
    "tech_support", "streaming_tv", "streaming_movies"
)

dim_contract_df = df.select(
    "customerid", "contract", "paperless_billing", "payment_method"
)

fact_churn_df = df.select(
    "customerid", "tenure_months", "monthly_charges", "total_charges",
    "churn_label", "churn_value", "churn_score", "churn_reason"
)

# ---- 6. WRITE TO REDSHIFT ----------------------------------------------------------------------------------
redshift_options = {
    "redshiftTmpDir": "s3://aws-glue-assets-206470328195-us-east-1/temporary/",
    "useConnectionProperties": "true",
    "connectionName": "Jdbc connection"
}

def write_to_redshift(dataframe, table_name):
    logger.info(f"Writing to Redshift table: {table_name}")
    dyf = DynamicFrame.fromDF(dataframe, glueContext, table_name)
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="redshift",
        connection_options={
            **redshift_options,
            "dbtable": f"public.{table_name}",
            "preactions": f"TRUNCATE TABLE public.{table_name};"
        },
        transformation_ctx=table_name
    )
    logger.info(f"Successfully written to {table_name}")

write_to_redshift(dim_customer_df, "dim_customer")
write_to_redshift(dim_services_df, "dim_services")
write_to_redshift(dim_contract_df, "dim_contract")
write_to_redshift(fact_churn_df, "fact_churn")

logger.info("Glue Job completed successfully")
job.commit()