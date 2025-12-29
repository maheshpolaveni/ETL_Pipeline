# Retail Sales ETL Pipeline with Databricks

A comprehensive ETL pipeline for processing retail sales data using Databricks, PySpark, and Delta Lake. This pipeline ingests data from AWS S3, performs transformations (cleansing, deduplication, aggregation), loads into Delta Lake tables, and includes comprehensive data quality checks.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Configuration](#configuration)
- [Usage](#usage)
- [Pipeline Components](#pipeline-components)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## Architecture Overview

The pipeline follows a standard ETL pattern with the following flow:

```
AWS S3 (Raw Data) → Databricks (Ingestion) → Transformation → Delta Lake → Quality Checks → Reporting Tables
```

### Data Layers

- **Bronze Layer**: Raw ingested data from S3
- **Silver Layer**: Cleaned and transformed data
- **Gold Layer**: Aggregated business metrics

## Features

- **Incremental Data Processing**: Supports both full and incremental loads with date-based filtering
- **Data Validation and Quality Metrics**: Comprehensive quality checks including record counts, null percentages, data freshness, and business rules
- **Automated Job Scheduling**: Configurable Databricks jobs with cron-based scheduling
- **Error Handling**: Robust error handling with retry logic and dead letter queues
- **Delta Lake Integration**: ACID transactions, time travel, and optimized storage
- **Parameterized Execution**: Supports both interactive notebook runs and scheduled job runs

## Project Structure

```
retail-sales-etl-pipeline/
├── notebooks/
│   ├── 01_ingestion/
│   │   └── ingest_sales_data.py          # Data ingestion from S3
│   ├── 02_transformation/
│   │   └── transform_sales_data.py       # Data transformation
│   ├── 03_quality_checks/
│   │   └── data_quality_validation.py   # Quality validation
│   └── 04_orchestration/
│       └── main_etl_pipeline.py          # Main orchestration
├── src/
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config.py                     # Configuration management
│   │   ├── data_quality.py               # Quality check functions
│   │   └── s3_utils.py                   # S3 utility functions
│   └── transformations/
│       ├── __init__.py
│       ├── cleansing.py                  # Data cleansing
│       ├── deduplication.py              # Deduplication logic
│       └── aggregation.py                 # Aggregation functions
├── config/
│   ├── databricks_config.json            # Databricks job config
│   └── pipeline_config.yaml              # Pipeline configuration
├── tests/
│   ├── test_transformations.py           # Transformation tests
│   └── test_quality_checks.py            # Quality check tests
├── data/
│   └── sample/
│       └── sample_sales_data.csv         # Sample data
├── jobs/
│   └── etl_job_config.json               # Job definition
├── README.md
└── requirements.txt
```

## Prerequisites

- **Databricks Workspace**: Azure Databricks workspace with appropriate permissions
- **AWS S3 Access**: S3 bucket with read permissions (or use IAM roles)
- **Python 3.8+**: For local development and testing
- **Databricks CLI** (optional): For deploying notebooks and jobs

### Required Databricks Runtime

- Databricks Runtime 13.3 LTS or higher
- Delta Lake support (included in runtime)
- PySpark 3.4+

## Setup Instructions

### 1. Clone or Upload Project to Databricks

#### Option A: Using Databricks Repos (Recommended)

1. In Databricks workspace, go to **Repos**
2. Click **Add Repo** and connect to your Git repository
3. Clone the repository or upload the project files

#### Option B: Manual Upload

1. Use Databricks CLI or UI to upload notebooks to `/notebooks/` directory
2. Upload `src/` directory to workspace or mount point
3. Upload `config/` directory

### 2. Configure AWS Credentials

#### Option A: IAM Role (Recommended for Production)

1. Attach IAM role to Databricks cluster with S3 read permissions
2. No additional configuration needed

#### Option B: Access Keys

Set environment variables or Spark configuration:

```python
spark.conf.set("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY")
```

### 3. Install Dependencies

The required libraries are typically pre-installed in Databricks runtime. For custom libraries, add to cluster or job:

```json
{
  "libraries": [
    {"pypi": {"package": "boto3==1.34.0"}},
    {"pypi": {"package": "pyyaml==6.0.1"}}
  ]
}
```

### 4. Configure Pipeline Settings

Edit `config/pipeline_config.yaml` with your specific settings:

```yaml
s3:
  bucket: your-bucket-name
  raw_data_path: s3://your-bucket/raw/
  region: us-east-1

delta:
  database: retail_sales
  bronze_table: sales_bronze
  silver_table: sales_silver
  table_path: /dbfs/mnt/retail-sales/delta/
```

### 5. Set Up Delta Database

Run in a Databricks notebook:

```sql
CREATE DATABASE IF NOT EXISTS retail_sales;
USE retail_sales;
```

## Configuration

### Pipeline Configuration (`config/pipeline_config.yaml`)

Key configuration options:

- **S3 Settings**: Bucket name, paths, region
- **Delta Settings**: Table names, database, storage paths
- **Quality Thresholds**: Null percentages, record counts, data freshness
- **Business Rules**: Validation rules for data quality

### Databricks Job Configuration (`jobs/etl_job_config.json`)

Configure:
- **Schedule**: Cron expression for automated runs
- **Cluster Configuration**: Node types, worker count, Spark settings
- **Parameters**: Default parameters for job runs
- **Notifications**: Email/webhook notifications

## Usage

### Interactive Notebook Execution

1. Open any notebook in Databricks
2. Set parameters using widgets or edit directly
3. Run all cells or execute specific steps

Example: Run ingestion notebook

```python
# Parameters are set via widgets
source_path = "s3://retail-sales-data/raw/"
target_date = "2024-01-15"
load_type = "incremental"
```

### Scheduled Job Execution

1. Create a Databricks job using `jobs/etl_job_config.json`
2. Configure schedule (default: daily at 2 AM)
3. Job will run automatically with configured parameters

#### Creating Job via Databricks CLI

```bash
databricks jobs create --json-file jobs/etl_job_config.json
```

#### Creating Job via UI

1. Go to **Jobs** → **Create Job**
2. Import configuration from `jobs/etl_job_config.json`
3. Adjust parameters as needed
4. Save and enable schedule

### Manual Job Run

```bash
databricks jobs run-now --job-id <job-id> \
  --notebook-params '{"source_path": "s3://bucket/raw/", "target_date": "2024-01-15"}'
```

## Pipeline Components

### 1. Data Ingestion (`01_ingestion/ingest_sales_data.py`)

- Reads CSV/JSON files from AWS S3
- Supports incremental and full loads
- Logs ingestion metrics
- Writes to Bronze Delta table

**Features:**
- Automatic file discovery
- Date-based filtering for incremental loads
- Error handling for missing/corrupted files
- Ingestion metadata tracking

### 2. Data Transformation (`02_transformation/transform_sales_data.py`)

**Cleansing:**
- Removes null/invalid records
- Standardizes date formats
- Cleans text fields (trim, case conversion)
- Handles missing values with business rules

**Deduplication:**
- Identifies duplicates by business keys
- Keeps latest record based on timestamp
- Logs duplicate statistics

**Aggregation:**
- Daily/weekly/monthly sales summaries
- Product-level aggregations
- Customer-level aggregations
- Revenue calculations

**Output:** Silver Delta table with cleaned, deduplicated data

### 3. Data Quality Checks (`03_quality_checks/data_quality_validation.py`)

**Checks:**
- Record count validation (expected vs actual)
- Null percentage thresholds
- Data freshness (max timestamp validation)
- Business rule validation
- Referential integrity

**Output:** Quality metrics report and pass/fail status

### 4. Main Orchestration (`04_orchestration/main_etl_pipeline.py`)

- Coordinates all pipeline steps
- Parameterized for job scheduling
- Error handling and retry logic
- Execution logging
- Pipeline status tracking

## Testing

### Running Unit Tests

Tests are located in `tests/` directory. To run locally:

```bash
# Install test dependencies
pip install pytest pytest-spark

# Run tests
pytest tests/ -v
```

### Test Coverage

- **Transformation Tests**: Cleansing, deduplication, aggregation
- **Quality Check Tests**: All validation functions
- **Integration Tests**: End-to-end pipeline (requires Databricks environment)

### Sample Data

Sample data with edge cases is provided in `data/sample/sample_sales_data.csv`:
- Null values in various columns
- Duplicate transactions
- Invalid data (negative amounts, zero quantities)

## Troubleshooting

### Common Issues

#### 1. S3 Access Denied

**Error:** `AccessDenied` when reading from S3

**Solution:**
- Verify IAM role has S3 read permissions
- Check bucket policy allows Databricks access
- Verify AWS credentials if using access keys

#### 2. Delta Table Not Found

**Error:** `Table or view not found`

**Solution:**
- Ensure database exists: `CREATE DATABASE IF NOT EXISTS retail_sales;`
- Check table path in configuration
- Verify table was created in previous run

#### 3. Quality Checks Failing

**Error:** Quality checks fail with high null percentage

**Solution:**
- Review data source for data quality issues
- Adjust quality thresholds in `pipeline_config.yaml`
- Check business rules configuration

#### 4. Out of Memory Errors

**Error:** `java.lang.OutOfMemoryError`

**Solution:**
- Increase cluster size (more workers or larger nodes)
- Enable Spark adaptive query execution
- Optimize Delta tables: `OPTIMIZE table_name`

#### 5. Import Errors

**Error:** `ModuleNotFoundError` for src modules

**Solution:**
- Ensure `src/` directory is in Python path
- Add to cluster libraries or workspace files
- Check import paths in notebooks

### Debugging Tips

1. **Check Execution Logs**: Review Databricks job run logs
2. **Verify Configuration**: Check `pipeline_config.yaml` values
3. **Test Individual Steps**: Run notebooks individually to isolate issues
4. **Review Delta Table History**: Use `DESCRIBE HISTORY table_name` to see changes
5. **Check Data Quality Metrics**: Query `data_quality_metrics` table for historical issues

### Getting Help

- Review Databricks documentation: https://docs.databricks.com/
- Check Delta Lake documentation: https://delta.io/
- Review PySpark documentation: https://spark.apache.org/docs/latest/api/python/

## Best Practices

1. **Incremental Processing**: Use incremental loads for large datasets to reduce processing time
2. **Partitioning**: Partition Delta tables by date for better query performance
3. **Optimization**: Regularly run `OPTIMIZE` and `ZORDER BY` on Delta tables
4. **Monitoring**: Set up alerts for job failures and quality check failures
5. **Version Control**: Use Databricks Repos for notebook version control
6. **Testing**: Test transformations on sample data before production runs

## License

This project is provided as-is for educational and demonstration purposes.

## Contributing

Contributions are welcome! Please ensure:
- Code follows PEP 8 style guidelines
- Tests are included for new features
- Documentation is updated

## Contact

For questions or issues, please open an issue in the repository.

