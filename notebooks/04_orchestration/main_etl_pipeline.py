# Databricks notebook source
# MAGIC %md
# MAGIC # Main ETL Pipeline Orchestration
# MAGIC 
# MAGIC This notebook orchestrates the complete ETL pipeline:
# MAGIC 1. Data Ingestion
# MAGIC 2. Data Transformation
# MAGIC 3. Data Quality Checks
# MAGIC 
# MAGIC Supports parameterized runs for job scheduling.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from datetime import datetime
import json
import sys
import os

# Add src to path for imports
sys.path.append('/Workspace/Repos/retail-sales-etl-pipeline/src')
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from utils.config import config
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Parameters

# COMMAND ----------

# Create widgets for parameters (for interactive runs)
dbutils.widgets.text("source_path", config.get('s3.raw_data_path', 's3://retail-sales-data/raw/'), "Source S3 Path")
dbutils.widgets.text("target_date", datetime.now().strftime('%Y-%m-%d'), "Target Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load Type")
dbutils.widgets.text("file_format", "csv", "File Format (csv/json)")
dbutils.widgets.dropdown("fail_on_quality_failure", "true", ["true", "false"], "Fail on Quality Check Failure")

# Get parameter values
source_path = dbutils.widgets.get("source_path")
target_date = dbutils.widgets.get("target_date")
load_type = dbutils.widgets.get("load_type")
file_format = dbutils.widgets.get("file_format")
fail_on_quality_failure = dbutils.widgets.get("fail_on_quality_failure").lower() == "true"

# Pipeline metadata
pipeline_start_time = datetime.now()
pipeline_run_id = f"etl_{pipeline_start_time.strftime('%Y%m%d_%H%M%S')}"

logger.info(f"Starting ETL pipeline run: {pipeline_run_id}")
logger.info(f"Parameters - Source: {source_path}, Date: {target_date}, Type: {load_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution Tracking

# COMMAND ----------

# Initialize pipeline results
pipeline_results = {
    'run_id': pipeline_run_id,
    'start_time': pipeline_start_time.isoformat(),
    'parameters': {
        'source_path': source_path,
        'target_date': target_date,
        'load_type': load_type,
        'file_format': file_format
    },
    'steps': {},
    'status': 'RUNNING',
    'errors': []
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Data Ingestion

# COMMAND ----------

ingestion_start = datetime.now()

try:
    logger.info("="*60)
    logger.info("STEP 1: DATA INGESTION")
    logger.info("="*60)
    
    # Run ingestion notebook
    ingestion_result = dbutils.notebook.run(
        "../01_ingestion/ingest_sales_data",
        timeout_seconds=3600,
        arguments={
            "source_path": source_path,
            "target_date": target_date,
            "load_type": load_type,
            "file_format": file_format
        }
    )
    
    ingestion_end = datetime.now()
    ingestion_duration = (ingestion_end - ingestion_start).total_seconds()
    
    # Parse results (notebook returns metrics as string)
    try:
        ingestion_metrics = eval(ingestion_result) if isinstance(ingestion_result, str) else ingestion_result
    except:
        ingestion_metrics = {"raw_result": ingestion_result}
    
    pipeline_results['steps']['ingestion'] = {
        'status': 'SUCCESS',
        'duration_seconds': ingestion_duration,
        'metrics': ingestion_metrics
    }
    
    logger.info(f"✓ Ingestion completed in {ingestion_duration:.2f} seconds")
    
except Exception as e:
    ingestion_end = datetime.now()
    ingestion_duration = (ingestion_end - ingestion_start).total_seconds()
    
    error_msg = f"Ingestion failed: {str(e)}"
    logger.error(error_msg)
    
    pipeline_results['steps']['ingestion'] = {
        'status': 'FAILED',
        'duration_seconds': ingestion_duration,
        'error': error_msg
    }
    pipeline_results['errors'].append(error_msg)
    pipeline_results['status'] = 'FAILED'
    
    # Fail pipeline if ingestion fails
    raise Exception(error_msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Data Transformation

# COMMAND ----------

transformation_start = datetime.now()

try:
    logger.info("="*60)
    logger.info("STEP 2: DATA TRANSFORMATION")
    logger.info("="*60)
    
    # Get table names from config
    bronze_table = config.get('delta.bronze_table', 'sales_bronze')
    silver_table = config.get('delta.silver_table', 'sales_silver')
    
    # Run transformation notebook
    transformation_result = dbutils.notebook.run(
        "../02_transformation/transform_sales_data",
        timeout_seconds=3600,
        arguments={
            "bronze_table": bronze_table,
            "silver_table": silver_table,
            "target_date": target_date if target_date else ""
        }
    )
    
    transformation_end = datetime.now()
    transformation_duration = (transformation_end - transformation_start).total_seconds()
    
    # Parse results
    try:
        transformation_metrics = eval(transformation_result) if isinstance(transformation_result, str) else transformation_result
    except:
        transformation_metrics = {"raw_result": transformation_result}
    
    pipeline_results['steps']['transformation'] = {
        'status': 'SUCCESS',
        'duration_seconds': transformation_duration,
        'metrics': transformation_metrics
    }
    
    logger.info(f"✓ Transformation completed in {transformation_duration:.2f} seconds")
    
except Exception as e:
    transformation_end = datetime.now()
    transformation_duration = (transformation_end - transformation_start).total_seconds()
    
    error_msg = f"Transformation failed: {str(e)}"
    logger.error(error_msg)
    
    pipeline_results['steps']['transformation'] = {
        'status': 'FAILED',
        'duration_seconds': transformation_duration,
        'error': error_msg
    }
    pipeline_results['errors'].append(error_msg)
    pipeline_results['status'] = 'FAILED'
    
    # Fail pipeline if transformation fails
    raise Exception(error_msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Data Quality Checks

# COMMAND ----------

quality_start = datetime.now()

try:
    logger.info("="*60)
    logger.info("STEP 3: DATA QUALITY CHECKS")
    logger.info("="*60)
    
    silver_table = config.get('delta.silver_table', 'sales_silver')
    
    # Run quality checks notebook
    quality_result = dbutils.notebook.run(
        "../03_quality_checks/data_quality_validation",
        timeout_seconds=1800,
        arguments={
            "silver_table": silver_table,
            "target_date": target_date if target_date else "",
            "fail_on_critical": str(fail_on_quality_failure).lower()
        }
    )
    
    quality_end = datetime.now()
    quality_duration = (quality_end - quality_start).total_seconds()
    
    # Parse results
    try:
        quality_metrics = json.loads(quality_result) if isinstance(quality_result, str) else quality_result
    except:
        quality_metrics = {"raw_result": quality_result}
    
    quality_status = quality_metrics.get('overall_status', 'UNKNOWN')
    checks_passed = quality_metrics.get('checks_passed', False)
    
    pipeline_results['steps']['quality_checks'] = {
        'status': 'SUCCESS' if checks_passed else 'WARNING',
        'duration_seconds': quality_duration,
        'overall_status': quality_status,
        'metrics': quality_metrics
    }
    
    logger.info(f"✓ Quality checks completed in {quality_duration:.2f} seconds")
    logger.info(f"  Overall status: {quality_status}")
    
    # Fail pipeline if quality checks fail and fail_on_quality_failure is True
    if not checks_passed and fail_on_quality_failure:
        error_msg = f"Quality checks failed with status: {quality_status}"
        logger.error(error_msg)
        pipeline_results['status'] = 'FAILED'
        pipeline_results['errors'].append(error_msg)
        raise Exception(error_msg)
    
except Exception as e:
    quality_end = datetime.now()
    quality_duration = (quality_end - quality_start).total_seconds()
    
    error_msg = f"Quality checks failed: {str(e)}"
    logger.error(error_msg)
    
    pipeline_results['steps']['quality_checks'] = {
        'status': 'FAILED',
        'duration_seconds': quality_duration,
        'error': error_msg
    }
    pipeline_results['errors'].append(error_msg)
    
    # Only fail pipeline if fail_on_quality_failure is True
    if fail_on_quality_failure:
        pipeline_results['status'] = 'FAILED'
        raise Exception(error_msg)
    else:
        logger.warning("Quality checks failed, but continuing pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

pipeline_end_time = datetime.now()
pipeline_duration = (pipeline_end_time - pipeline_start_time).total_seconds()

# Update final status
if pipeline_results['status'] != 'FAILED':
    pipeline_results['status'] = 'SUCCESS'

pipeline_results['end_time'] = pipeline_end_time.isoformat()
pipeline_results['total_duration_seconds'] = pipeline_duration

# Calculate step durations
step_summary = {}
for step_name, step_result in pipeline_results['steps'].items():
    step_summary[step_name] = {
        'status': step_result.get('status'),
        'duration_seconds': step_result.get('duration_seconds', 0)
    }

print("\n" + "="*60)
print("ETL PIPELINE EXECUTION SUMMARY")
print("="*60)
print(f"Run ID: {pipeline_run_id}")
print(f"Status: {pipeline_results['status']}")
print(f"Total Duration: {pipeline_duration:.2f} seconds")
print(f"\nStep Summary:")
for step_name, step_info in step_summary.items():
    status_icon = "✓" if step_info['status'] == 'SUCCESS' else "✗" if step_info['status'] == 'FAILED' else "⚠"
    print(f"  {status_icon} {step_name}: {step_info['status']} ({step_info['duration_seconds']:.2f}s)")

if pipeline_results['errors']:
    print(f"\nErrors ({len(pipeline_results['errors'])}):")
    for error in pipeline_results['errors']:
        print(f"  - {error}")

print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Pipeline Execution Log

# COMMAND ----------

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    from pyspark.sql.functions import current_timestamp as spark_current_timestamp
    
    spark = SparkSession.builder.appName("ETLPipelineOrchestration").getOrCreate()
    
    # Create execution log DataFrame
    execution_log_data = [{
        'run_id': pipeline_run_id,
        'start_time': pipeline_start_time,
        'end_time': pipeline_end_time,
        'duration_seconds': pipeline_duration,
        'status': pipeline_results['status'],
        'parameters': json.dumps(pipeline_results['parameters']),
        'step_summary': json.dumps(step_summary),
        'errors': json.dumps(pipeline_results['errors']),
        'log_timestamp': spark_current_timestamp()
    }]
    
    execution_log_df = spark.createDataFrame(execution_log_data)
    
    # Write to execution log table
    log_table = "etl_pipeline_execution_log"
    log_path = f"{config.get('delta.table_path', '/dbfs/mnt/retail-sales/delta/')}{log_table}"
    
    execution_log_df.write \
        .format("delta") \
        .mode("append") \
        .save(log_path)
    
    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {log_table}
        USING DELTA
        LOCATION '{log_path}'
    """)
    
    logger.info(f"✓ Pipeline execution log stored in {log_table}")
    
except Exception as e:
    logger.warning(f"Could not store execution log: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Status

# COMMAND ----------

# Return final status
if pipeline_results['status'] == 'FAILED':
    raise Exception(f"Pipeline failed: {', '.join(pipeline_results['errors'])}")
else:
    logger.info("✓ ETL Pipeline completed successfully!")
    dbutils.notebook.exit(json.dumps({
        'status': pipeline_results['status'],
        'run_id': pipeline_run_id,
        'duration_seconds': pipeline_duration,
        'steps': step_summary
    }))

