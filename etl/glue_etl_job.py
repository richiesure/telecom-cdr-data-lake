import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, current_timestamp, lit
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Job parameters
database_name = 'telecom_data_lake'
table_name = 'cdr_records'
s3_raw_path = 's3://YOUR-BUCKET-NAME/raw/cdr/'
s3_processed_path = 's3://YOUR-BUCKET-NAME/processed/parquet/cdr/'

print("="*60)
print("TELECOM CDR ETL JOB")
print("="*60)

# Define schema
schema = StructType([
    StructField("record_id", StringType(), True),
    StructField("msisdn", StringType(), True),
    StructField("imsi", StringType(), True),
    StructField("call_type", StringType(), True),
    StructField("call_status", StringType(), True),
    StructField("call_duration_seconds", IntegerType(), True),
    StructField("data_usage_mb", DoubleType(), True),
    StructField("cell_tower_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("call_timestamp", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("billing_amount", DoubleType(), True)
])

# Read CSV
raw_df = spark.read.option("header", "true").schema(schema).csv(s3_raw_path)

# Transform
transformed_df = raw_df.withColumn("call_date", to_date(col("call_date"), "yyyy-MM-dd"))
transformed_df = transformed_df.withColumn("processing_timestamp", current_timestamp())
transformed_df = transformed_df.withColumn("data_source", lit("raw_cdr_csv"))
transformed_df = transformed_df.withColumn("pipeline_version", lit("v1.0"))

# Data quality checks
clean_df = transformed_df.filter(col("record_id").isNotNull())
clean_df = clean_df.filter(col("msisdn").isNotNull())

# Write to Parquet
clean_df.write.mode("overwrite").partitionBy("call_date").option("compression", "snappy").parquet(s3_processed_path)

# Create table
spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
spark.sql(f"""
    CREATE EXTERNAL TABLE {database_name}.{table_name} (
        record_id STRING,
        msisdn STRING,
        imsi STRING,
        call_type STRING,
        call_status STRING,
        call_duration_seconds INT,
        data_usage_mb DOUBLE,
        cell_tower_id STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        call_timestamp STRING,
        billing_amount DOUBLE,
        processing_timestamp TIMESTAMP,
        data_source STRING,
        pipeline_version STRING
    )
    PARTITIONED BY (call_date DATE)
    STORED AS PARQUET
    LOCATION '{s3_processed_path}'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
""")

spark.sql(f"MSCK REPAIR TABLE {database_name}.{table_name}")

print("✓ ETL Job completed successfully!")
job.commit()
