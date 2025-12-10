# Telecom CDR Data Lake - AWS Production Pipeline

> Production-ready telecom Call Detail Record (CDR) data lake demonstrating L3 Data Engineering and AWS best practices.

## Overview

This project implements a scalable data lake for processing telecom CDRs using AWS Glue, S3, Athena, and PySpark.

**Key Features:**
- ETL pipeline with AWS Glue and PySpark
- S3 data lake with Parquet format (70% compression)
- Date-based partitioning (95% query cost reduction)
- Data quality engineering and monitoring
- L3 support documentation

## Tech Stack

- AWS Glue, S3, Athena, CloudWatch
- PySpark
- Python 3.9+
- Parquet format
- SQL

## Quick Start

1. Setup AWS resources (S3 bucket, Glue database)
2. Upload sample data to S3 raw zone
3. Deploy Glue ETL job
4. Query data with Athena

See `docs/setup_guide.md` for detailed instructions.

## Project Structure

```
telecom-cdr-data-lake/
├── etl/                    # ETL pipeline code
├── monitoring/             # Quality checks
├── sql/                    # Analytics queries
├── docs/                   # Documentation
└── config/                 # Configuration files
```

## Results

- Processing time: 1.5 minutes for 10K records
- Storage reduction: 70%
- Query performance: < 1 second
- Cost: ~$0.07 per run

## Author

**Your Name**
- Email: Richieprograms@gmail.com
- LinkedIn: (https://www.linkedin.com/in/efemenaisaac
