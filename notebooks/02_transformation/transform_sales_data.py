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

# ===============================
# TRANSFORMATION - INITIAL SETUP
# ===============================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import os
import sys
import logging
import json

REPO_ROOT = "/Workspace/Repos/maheshpolaveni96@gmail.com/ETL_Pipeline"
SRC_PATH = os.path.join(REPO_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)


from utils.config import config
from transformations.cleansing import DataCleansing
from transformations.deduplication import Deduplication
from transformations.aggregation import Aggregation

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_TRANSFORMATION")

start_time = datetime.now()
logger.info("Transformation notebook started")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# ============================
# Widgets for Transformation Parameters
# ============================

# Bronze & Silver Delta table names (catalog-qualified if using Unity Catalog)
dbutils.widgets.text("bronze_table", config.get('delta.bronze_table', 'workspace.retail.sales_bronze'), "Bronze Table Name")
dbutils.widgets.text("silver_table", config.get('delta.silver_table', 'workspace.retail.sales_silver'), "Silver Table Name")
dbutils.widgets.text("target_date", "", "Target Date (YYYY-MM-DD) - optional filter")

# Retrieve widget values
bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")
target_date = dbutils.widgets.get("target_date")

# Log the parameter values
logger.info(f"Transformation parameters:")
logger.info(f"  Bronze table: {bronze_table}")
logger.info(f"  Silver table: {silver_table}")
logger.info(f"  Target date filter: {target_date if target_date else 'None'}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Bronze Layer

# COMMAND ----------

bronze_table = "retail.sales_bronze"
silver_table = "retail.sales_silver"

df_bronze = spark.table(bronze_table)

if target_date:
    df_bronze = df_bronze.filter(col("sale_date") == target_date)

initial_count = df_bronze.count()
logger.info(f"Read {initial_count} bronze records")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Data Cleansing

# COMMAND ----------

pipeline_config = config.get_pipeline_config()

cleansing_config = {
    "remove_nulls": True,
    "null_columns": ["transaction_id", "sale_date", "sale_amount"],
    "remove_invalid": True,
    "validation_rules": {
        "sale_amount": col("sale_amount") > 0,
        "quantity": col("quantity") >= 0
    }
}

df_cleansed = DataCleansing.clean_all(df_bronze, cleansing_config)
cleansed_count = df_cleansed.count()

logger.info(f"Cleansed: {initial_count} → {cleansed_count}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Deduplication

# COMMAND ----------

df_deduped = Deduplication.remove_duplicates_by_transaction_id(
    df_cleansed,
    transaction_id_col="transaction_id",
    timestamp_col="sale_date"
)

deduped_count = df_deduped.count()
duplicates_removed = cleansed_count - deduped_count

logger.info(f"Deduplicated: removed {duplicates_removed}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Revenue Calculations

# COMMAND ----------

df_final = Aggregation.revenue_calculations(
    df_deduped,
    amount_column="sale_amount",
    quantity_column="quantity"
)

final_count = df_final.count()
logger.info(f"Revenue calculated on {final_count} records")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Silver Layer

# COMMAND ----------

from pyspark.sql.functions import col, try_divide

class Aggregation:

    @staticmethod
    def revenue_calculations(df, amount_column, quantity_column):
        return (
            df
            .withColumn(
                "net_revenue",
                col(amount_column) * col(quantity_column)
            )
            .withColumn(
                "unit_price",
                try_divide(col(amount_column), col(quantity_column))
            )
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Summary

# COMMAND ----------

end_time = datetime.now()
duration_seconds = (end_time - start_time).total_seconds()

result = {
    "status": "SUCCESS",
    "duration_seconds": round(duration_seconds, 2),
    "metrics": {
        "bronze_records": initial_count,
        "cleansed_records": cleansed_count,
        "deduped_records": deduped_count,
        "duplicates_removed": duplicates_removed,
        "silver_table": silver_table
    }
}

dbutils.notebook.exit(json.dumps(result))

