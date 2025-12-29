# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Validation Notebook
# MAGIC 
# MAGIC This notebook runs comprehensive data quality checks:
# MAGIC - Record count validation
# MAGIC - Null percentage checks
# MAGIC - Data freshness validation
# MAGIC - Business rule validation
# MAGIC - Referential integrity checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os
import json

# Add src to path for imports
sys.path.append('/Workspace/Repos/retail-sales-etl-pipeline/src')
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from utils.config import config
from utils.data_quality import DataQuality
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("silver_table", config.get('delta.silver_table', 'sales_silver'), "Silver Table Name")
dbutils.widgets.text("target_date", "", "Target Date (YYYY-MM-DD) - optional filter")
dbutils.widgets.dropdown("fail_on_critical", "true", ["true", "false"], "Fail Pipeline on Critical Check Failure")

# Get parameter values
silver_table = dbutils.widgets.get("silver_table")
target_date = dbutils.widgets.get("target_date")
fail_on_critical = dbutils.widgets.get("fail_on_critical").lower() == "true"

logger.info(f"Quality check parameters - Table: {silver_table}, Fail on critical: {fail_on_critical}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Silver Layer

# COMMAND ----------

spark = SparkSession.builder.appName("RetailSalesQualityChecks").getOrCreate()

# Read from silver table
try:
    df_silver = spark.read.format("delta").table(silver_table)
    
    # Apply date filter if provided
    if target_date:
        df_silver = df_silver.filter(col("sale_date") == target_date)
        logger.info(f"Filtered data for date: {target_date}")
    
    record_count = df_silver.count()
    logger.info(f"Read {record_count} records from silver table: {silver_table}")
    
    if record_count == 0:
        raise ValueError(f"No records found in {silver_table} for quality checks")
    
    print(f"\nData sample ({record_count} total records):")
    df_silver.show(5, truncate=False)
    
except Exception as e:
    logger.error(f"Error reading from silver table: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Quality Checks

# COMMAND ----------

# Get configuration
pipeline_config = config.get_pipeline_config()
quality_config = config.get_quality_config()
quality_thresholds = pipeline_config.get('quality_thresholds', {})
business_rules_config = pipeline_config.get('business_rules', {})

# Build quality check configuration
quality_check_config = {
    'check_record_count': True,
    'min_count': quality_thresholds.get('min_record_count', 100),
    'check_nulls': True,
    'null_check_columns': ['transaction_id', 'sale_date', 'sale_amount', 'customer_id', 'product_id'],
    'null_threshold': quality_thresholds.get('null_percentage', 0.05),
    'check_freshness': True,
    'timestamp_column': pipeline_config.get('date_column', 'sale_date'),
    'max_age_hours': quality_thresholds.get('max_age_hours', 24),
    'check_business_rules': True,
    'business_rules': {
        'sale_amount': {
            'min': business_rules_config.get('min_sale_amount', 0.01),
            'max': business_rules_config.get('max_sale_amount', 100000)
        },
        'quantity': {
            'min': 1
        }
    },
    'critical_checks': quality_config.get('critical_checks', ['record_count', 'null_percentage', 'data_freshness'])
}

# Add region validation if configured
if 'valid_regions' in business_rules_config:
    quality_check_config['business_rules']['region'] = {
        'allowed_values': business_rules_config['valid_regions']
    }

print("\nQuality check configuration:")
print(json.dumps(quality_check_config, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Data Quality Checks

# COMMAND ----------

try:
    # Run all quality checks
    quality_results = DataQuality.run_all_checks(df_silver, quality_check_config)
    
    print("\n=== Data Quality Check Results ===")
    print(json.dumps(quality_results, indent=2, default=str))
    
except Exception as e:
    logger.error(f"Error running quality checks: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Check Results

# COMMAND ----------

# Display individual check results
for check_name, check_result in quality_results.items():
    if check_name in ['overall_status', 'summary']:
        continue
    
    status = check_result.get('status', 'UNKNOWN')
    message = check_result.get('message', '')
    
    status_icon = "✓" if status == "PASS" else "✗" if status == "FAIL" else "⚠"
    
    print(f"\n{status_icon} {check_name.upper()}: {status}")
    print(f"   {message}")
    
    # Show details for failed checks
    if status == "FAIL":
        if 'null_percentages' in check_result:
            print("   Null percentages by column:")
            for col_name, pct in check_result['null_percentages'].items():
                if pct > quality_check_config.get('null_threshold', 0.05):
                    print(f"     - {col_name}: {pct:.2%}")
        
        if 'failed_rules' in check_result:
            print("   Failed business rules:")
            for rule in check_result['failed_rules']:
                print(f"     - {rule}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall Quality Summary

# COMMAND ----------

overall_status = quality_results.get('overall_status', 'UNKNOWN')
summary = quality_results.get('summary', {})

print("\n" + "="*50)
print(f"OVERALL QUALITY STATUS: {overall_status}")
print("="*50)
print(f"Total Checks: {summary.get('total_checks', 0)}")
print(f"  ✓ Passed: {summary.get('passed', 0)}")
print(f"  ✗ Failed: {summary.get('failed', 0)}")
print(f"  ⚠ Warnings: {summary.get('warnings', 0)}")
print("="*50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Quality Metrics

# COMMAND ----------

# Store quality metrics in a Delta table for tracking
try:
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    from pyspark.sql.functions import current_timestamp as spark_current_timestamp
    
    # Create metrics DataFrame
    metrics_data = [{
        'check_timestamp': spark_current_timestamp(),
        'table_name': silver_table,
        'target_date': target_date if target_date else 'all',
        'overall_status': overall_status,
        'total_checks': summary.get('total_checks', 0),
        'passed_checks': summary.get('passed', 0),
        'failed_checks': summary.get('failed', 0),
        'warning_checks': summary.get('warnings', 0),
        'quality_results': json.dumps(quality_results, default=str)
    }]
    
    metrics_df = spark.createDataFrame(metrics_data)
    
    # Write to quality metrics table
    metrics_table = "data_quality_metrics"
    metrics_path = f"{config.get('delta.table_path', '/dbfs/mnt/retail-sales/delta/')}{metrics_table}"
    
    metrics_df.write \
        .format("delta") \
        .mode("append") \
        .save(metrics_path)
    
    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {metrics_table}
        USING DELTA
        LOCATION '{metrics_path}'
    """)
    
    print(f"\n✓ Quality metrics stored in {metrics_table}")
    
except Exception as e:
    logger.warning(f"Could not store quality metrics: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Critical Failures

# COMMAND ----------

# Fail pipeline if critical checks fail and fail_on_critical is True
if overall_status == "FAIL" and fail_on_critical:
    critical_failures = [
        check_name for check_name, result in quality_results.items()
        if isinstance(result, dict) and 
           result.get('status') == 'FAIL' and 
           check_name in quality_check_config.get('critical_checks', [])
    ]
    
    if critical_failures:
        error_message = f"Critical quality checks failed: {', '.join(critical_failures)}"
        logger.error(error_message)
        raise ValueError(error_message)
    else:
        logger.warning("Some quality checks failed, but no critical checks failed")
        print("\n⚠ Some quality checks failed, but pipeline will continue")
else:
    if overall_status == "PASS":
        print("\n✓ All quality checks passed!")
    else:
        print("\n⚠ Some quality checks failed, but pipeline will continue")

# Return results for downstream use
dbutils.notebook.exit(json.dumps({
    'overall_status': overall_status,
    'summary': summary,
    'checks_passed': overall_status == "PASS"
}))

