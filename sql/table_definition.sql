-- Glue Catalog Table Definition
DROP TABLE IF EXISTS telecom_data_lake.cdr_records;

CREATE EXTERNAL TABLE telecom_data_lake.cdr_records (
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
LOCATION 's3://YOUR-BUCKET-NAME/processed/parquet/cdr/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

MSCK REPAIR TABLE telecom_data_lake.cdr_records;
