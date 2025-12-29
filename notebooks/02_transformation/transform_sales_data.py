# Databricks notebook source
# MAGIC %md
# MAGIC # Data Transformation Notebook
# MAGIC 
# MAGIC This notebook performs data transformations:
# MAGIC - Cleansing (null handling, date standardization, text cleaning)
# MAGIC - Deduplication (remove duplicate records)
# MAGIC - Aggregation (daily/weekly/monthly summaries, product/customer aggregations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import sys
import os

# Add src to path for imports
sys.path.append('/Workspace/Repos/retail-sales-etl-pipeline/src')
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from utils.config import config
from transformations.cleansing import DataCleansing
from transformations.deduplication import Deduplication
from transformations.aggregation import Aggregation
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("bronze_table", config.get('delta.bronze_table', 'sales_bronze'), "Bronze Table Name")
dbutils.widgets.text("silver_table", config.get('delta.silver_table', 'sales_silver'), "Silver Table Name")
dbutils.widgets.text("target_date", "", "Target Date (YYYY-MM-DD) - optional filter")

# Get parameter values
bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")
target_date = dbutils.widgets.get("target_date")

logger.info(f"Transformation parameters - Bronze: {bronze_table}, Silver: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Bronze Layer

# COMMAND ----------

spark = SparkSession.builder.appName("RetailSalesTransformation").getOrCreate()

# Read from bronze table
try:
    df_bronze = spark.read.format("delta").table(bronze_table)
    
    # Apply date filter if provided
    if target_date:
        df_bronze = df_bronze.filter(col("sale_date") == target_date)
        logger.info(f"Filtered data for date: {target_date}")
    
    initial_count = df_bronze.count()
    logger.info(f"Read {initial_count} records from bronze table: {bronze_table}")
    
    print(f"\nBronze data sample:")
    df_bronze.show(5, truncate=False)
    
except Exception as e:
    logger.error(f"Error reading from bronze table: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Data Cleansing

# COMMAND ----------

# Get cleansing configuration
pipeline_config = config.get_pipeline_config()
date_column = pipeline_config.get('date_column', 'sale_date')

# Configure cleansing
cleansing_config = {
    'remove_nulls': True,
    'null_columns': ['transaction_id', 'sale_date', 'sale_amount'],  # Critical columns
    'standardize_dates': True,
    'date_columns': {date_column: 'yyyy-MM-dd'},
    'clean_text': True,
    'text_columns': ['region', 'store_id'],
    'trim_whitespace': True,
    'uppercase': False,
    'handle_missing': True,
    'fill_strategy': {
        'quantity': 'zero',
        'discount': 'zero',
        'region': 'Unknown'
    },
    'remove_invalid': True,
    'validation_rules': {
        'sale_amount': col('sale_amount') > 0,
        'quantity': col('quantity') > 0
    }
}

# Apply cleansing
try:
    df_cleansed = DataCleansing.clean_all(df_bronze, cleansing_config)
    cleansed_count = df_cleansed.count()
    
    logger.info(f"Cleansing complete: {initial_count} -> {cleansed_count} records")
    print(f"\n✓ Cleansed data: {cleansed_count} records")
    
    # Show sample
    df_cleansed.show(5, truncate=False)
    
except Exception as e:
    logger.error(f"Error during cleansing: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Deduplication

# COMMAND ----------

# Configure deduplication
dedup_config = {
    'key_columns': ['transaction_id'],
    'timestamp_column': 'sale_date',
    'keep': 'latest'
}

try:
    # Remove duplicates by transaction_id
    df_deduped = Deduplication.remove_duplicates_by_transaction_id(
        df_cleansed,
        transaction_id_col='transaction_id',
        timestamp_col='sale_date'
    )
    
    deduped_count = df_deduped.count()
    duplicates_removed = cleansed_count - deduped_count
    
    logger.info(f"Deduplication complete: Removed {duplicates_removed} duplicates")
    print(f"\n✓ Deduplication: {deduped_count} records (removed {duplicates_removed} duplicates)")
    
    # Get duplicate summary
    dup_summary = Deduplication.get_duplicate_summary(df_cleansed, ['transaction_id'])
    print(f"  Duplicate summary: {dup_summary}")
    
except Exception as e:
    logger.error(f"Error during deduplication: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Revenue Calculations

# COMMAND ----------

try:
    # Add revenue calculations
    df_with_revenue = Aggregation.revenue_calculations(
        df_deduped,
        amount_column='sale_amount',
        quantity_column='quantity'
    )
    
    print("\n✓ Added revenue calculations")
    df_with_revenue.select('transaction_id', 'sale_amount', 'quantity', 'net_revenue', 'unit_price').show(5)
    
except Exception as e:
    logger.error(f"Error adding revenue calculations: {e}")
    df_with_revenue = df_deduped  # Fallback

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Silver Layer

# COMMAND ----------

# Get Delta configuration
delta_config = config.get_delta_config()
table_path = delta_config.get('table_path', '/dbfs/mnt/retail-sales/delta/')
silver_path = f"{table_path}{silver_table}"
partition_column = pipeline_config.get('partition_column', 'sale_date')

try:
    # Write to Delta Lake (Silver layer) with merge for incremental updates
    df_with_revenue.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy(partition_column) \
        .save(silver_path)
    
    # Create/refresh table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_table}
        USING DELTA
        LOCATION '{silver_path}'
    """)
    
    # Optimize table
    spark.sql(f"OPTIMIZE {silver_table}")
    
    logger.info(f"Successfully wrote {deduped_count} records to silver table: {silver_table}")
    print(f"\n✓ Wrote {deduped_count} records to {silver_table}")
    print(f"  Location: {silver_path}")
    
except Exception as e:
    logger.error(f"Error writing to silver table: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Aggregations (Gold Layer)

# COMMAND ----------

# Get aggregation configuration
agg_config = {
    'date_column': date_column,
    'amount_column': 'sale_amount',
    'quantity_column': 'quantity',
    'daily_summary': True,
    'weekly_summary': True,
    'monthly_summary': True,
    'product_aggregation': True,
    'product_id_column': 'product_id',
    'product_grouping': ['region'],
    'customer_aggregation': True,
    'customer_id_column': 'customer_id'
}

try:
    # Create all aggregations
    aggregations = Aggregation.create_all_aggregations(df_with_revenue, agg_config)
    
    print(f"\n✓ Created {len(aggregations)} aggregation types:")
    for agg_type, agg_df in aggregations.items():
        count = agg_df.count()
        print(f"  - {agg_type}: {count} records")
        agg_df.show(3, truncate=False)
    
    # Write aggregations to Gold layer (optional)
    gold_table = config.get('delta.gold_table', 'sales_gold')
    gold_path = f"{table_path}{gold_table}"
    
    # For this example, we'll write daily summary to gold
    if 'daily' in aggregations:
        daily_agg = aggregations['daily']
        daily_agg.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("sale_date") \
            .save(f"{gold_path}_daily")
        
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {gold_table}_daily
            USING DELTA
            LOCATION '{gold_path}_daily'
        """)
        
        print(f"\n✓ Wrote daily aggregation to {gold_table}_daily")
    
except Exception as e:
    logger.error(f"Error creating aggregations: {e}")
    # Don't fail the pipeline if aggregations fail

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Summary

# COMMAND ----------

transformation_summary = {
    'bronze_records': initial_count,
    'cleansed_records': cleansed_count,
    'deduped_records': deduped_count,
    'duplicates_removed': duplicates_removed,
    'silver_table': silver_table,
    'transformations_applied': ['cleansing', 'deduplication', 'revenue_calculations', 'aggregations']
}

print("\n=== Transformation Summary ===")
for key, value in transformation_summary.items():
    print(f"{key}: {value}")

# Return summary for downstream use
dbutils.notebook.exit(str(transformation_summary))

