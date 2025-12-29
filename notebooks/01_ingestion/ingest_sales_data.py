# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion Notebook
# MAGIC 
# MAGIC This notebook ingests retail sales data from AWS S3 (CSV/JSON format).
# MAGIC Supports both full and incremental data loads.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp
from datetime import datetime, timedelta
import sys
import os

# Add src to path for imports
sys.path.append('/Workspace/Repos/retail-sales-etl-pipeline/src')
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from utils.config import config
from utils.s3_utils import S3Utils
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters (Widgets for Interactive Runs)

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("source_path", config.get('s3.raw_data_path', 's3://retail-sales-data/raw/'), "Source S3 Path")
dbutils.widgets.text("target_date", datetime.now().strftime('%Y-%m-%d'), "Target Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load Type")
dbutils.widgets.text("file_format", "csv", "File Format (csv/json)")

# Get parameter values
source_path = dbutils.widgets.get("source_path")
target_date = dbutils.widgets.get("target_date")
load_type = dbutils.widgets.get("load_type")
file_format = dbutils.widgets.get("file_format").lower()

# For job runs, parameters come from job configuration
# source_path = dbutils.widgets.get("source_path") or config.get('s3.raw_data_path')
# target_date = dbutils.widgets.get("target_date") or datetime.now().strftime('%Y-%m-%d')
# load_type = dbutils.widgets.get("load_type") or "incremental"

logger.info(f"Ingestion parameters - Source: {source_path}, Date: {target_date}, Type: {load_type}, Format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize S3 Connection

# COMMAND ----------

# Get S3 configuration
s3_config = config.get_s3_config()
bucket_name = s3_config.get('bucket', 'retail-sales-data')
region = s3_config.get('region', 'us-east-1')

# Initialize S3 utilities
# In Databricks, AWS credentials are typically configured via IAM roles or instance profiles
s3_utils = S3Utils(bucket_name=bucket_name, region=region)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover and List Source Files

# COMMAND ----------

# Extract bucket and prefix from S3 path
if source_path.startswith('s3://'):
    path_parts = source_path.replace('s3://', '').split('/', 1)
    bucket = path_parts[0]
    prefix = path_parts[1] if len(path_parts) > 1 else ''
else:
    bucket = bucket_name
    prefix = source_path

# List files based on load type
file_extensions = [f'.{file_format}']
start_time = datetime.now()

if load_type == 'incremental':
    # Get last processed timestamp (in production, this would come from Delta table metadata)
    # For now, use target_date to filter
    target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    last_processed = target_datetime - timedelta(days=1)
    
    files = s3_utils.get_incremental_files(
        prefix=prefix,
        last_processed_time=last_processed,
        file_extensions=file_extensions
    )
    logger.info(f"Incremental load: Found {len(files)} files modified since {last_processed}")
else:
    # Full load - get all files
    files = s3_utils.list_objects(prefix=prefix, file_extensions=file_extensions)
    logger.info(f"Full load: Found {len(files)} files")

# Display file list
if files:
    print(f"\nFiles to process ({len(files)}):")
    for f in files[:10]:  # Show first 10
        print(f"  - {f['key']} ({f['size']} bytes, modified: {f['last_modified']})")
    if len(files) > 10:
        print(f"  ... and {len(files) - 10} more files")
else:
    raise ValueError(f"No files found in {source_path} with format {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from S3

# COMMAND ----------

# Get Spark session
spark = SparkSession.builder.appName("RetailSalesIngestion").getOrCreate()

# Build S3 paths
s3_paths = [s3_utils.get_s3_path(f['key']) for f in files]
s3_path_pattern = ','.join(s3_paths) if len(s3_paths) <= 100 else source_path  # Use pattern if too many files

try:
    # Read data based on format
    if file_format == 'csv':
        df_raw = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .csv(s3_paths)
    elif file_format == 'json':
        df_raw = spark.read \
            .option("multiline", "true") \
            .json(s3_paths)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    # Add ingestion metadata
    df_raw = df_raw.withColumn("ingestion_timestamp", current_timestamp()) \
                   .withColumn("source_path", col("ingestion_timestamp").cast("string"))  # Simplified
    
    record_count = df_raw.count()
    logger.info(f"Successfully ingested {record_count} records from {len(files)} files")
    
    # Display sample data
    print(f"\nSample data ({record_count} total records):")
    df_raw.show(5, truncate=False)
    
    # Display schema
    print("\nSchema:")
    df_raw.printSchema()
    
except Exception as e:
    logger.error(f"Error reading data from S3: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Ingestion Metrics

# COMMAND ----------

# Calculate metrics
end_time = datetime.now()
duration_seconds = (end_time - start_time).total_seconds()
total_size_bytes = sum(f['size'] for f in files)
total_size_mb = total_size_bytes / (1024 * 1024)

ingestion_metrics = {
    'source_path': source_path,
    'load_type': load_type,
    'file_format': file_format,
    'files_processed': len(files),
    'records_ingested': record_count,
    'total_size_mb': round(total_size_mb, 2),
    'duration_seconds': round(duration_seconds, 2),
    'records_per_second': round(record_count / duration_seconds, 2) if duration_seconds > 0 else 0,
    'ingestion_timestamp': end_time.isoformat()
}

print("\n=== Ingestion Metrics ===")
for key, value in ingestion_metrics.items():
    print(f"{key}: {value}")

# Store metrics in a variable for downstream use
dbutils.notebook.exit(str(ingestion_metrics))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Layer (Delta Lake)

# COMMAND ----------

# Get Delta configuration
delta_config = config.get_delta_config()
bronze_table = delta_config.get('bronze_table', 'sales_bronze')
table_path = delta_config.get('table_path', '/dbfs/mnt/retail-sales/delta/')
bronze_path = f"{table_path}{bronze_table}"

# Write to Delta Lake (Bronze layer)
try:
    # For incremental loads, use merge; for full loads, overwrite
    if load_type == 'incremental' and spark._jsparkSession.catalog().tableExists(bronze_table):
        # Merge logic would go here - for now, append
        df_raw.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(bronze_path)
        logger.info(f"Appended data to bronze table: {bronze_table}")
    else:
        # Full load or first time - overwrite
        df_raw.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("sale_date") \
            .save(bronze_path)
        logger.info(f"Created/overwritten bronze table: {bronze_table}")
    
    # Create/refresh table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bronze_table}
        USING DELTA
        LOCATION '{bronze_path}'
    """)
    
    print(f"\n✓ Successfully wrote {record_count} records to {bronze_table}")
    print(f"  Location: {bronze_path}")
    
except Exception as e:
    logger.error(f"Error writing to Delta Lake: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Verify the write
try:
    verification_df = spark.read.format("delta").load(bronze_path)
    verification_count = verification_df.count()
    
    print(f"\n✓ Verification: {verification_count} records in bronze table")
    
    if verification_count != record_count:
        logger.warning(f"Record count mismatch: ingested {record_count}, verified {verification_count}")
    
except Exception as e:
    logger.error(f"Verification failed: {e}")

