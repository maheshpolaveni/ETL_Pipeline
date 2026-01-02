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

# =====================================
# ETL PIPELINE - INITIAL SETUP (UC SAFE)
# =====================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import sys, os, json, logging
from datetime import datetime

spark = SparkSession.builder.appName("RetailSalesIngestion").getOrCreate()

CATALOG_NAME = "workspace"
SCHEMA_NAME = "retail"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

REPO_ROOT = "/Workspace/Repos/maheshpolaveni96@gmail.com/ETL_Pipeline"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

logger = logging.getLogger("INGESTION")
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

from src.utils.config import config

logger.info("✓ Ingestion setup completed")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters (Widgets for Interactive Runs)

# COMMAND ----------

# =====================================
# PIPELINE PARAMETERS
# =====================================

dbutils.widgets.removeAll()

dbutils.widgets.text(
    "source_path",
    config.get("s3.raw_data_path", "s3://retail-sales-demo-mahesh/raw/"),
    "Source Path"
)

dbutils.widgets.text(
    "target_date",
    datetime.now().strftime("%Y-%m-%d"),
    "Target Date"
)

dbutils.widgets.dropdown(
    "load_type", "full", ["full", "incremental"], "Load Type"
)

dbutils.widgets.dropdown(
    "file_format", "csv", ["csv", "json"], "File Format"
)

source_path = dbutils.widgets.get("source_path")
target_date = dbutils.widgets.get("target_date")
load_type = dbutils.widgets.get("load_type")
file_format = dbutils.widgets.get("file_format")

logger.info(f"Params | source={source_path}, date={target_date}, type={load_type}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize S3 Connection

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from S3

# COMMAND ----------

# =====================================
# INGEST RAW DATA → BRONZE
# =====================================

import requests
import pandas as pd
from io import StringIO

BRONZE_TABLE = "workspace.retail.sales_bronze"
FILE_URL = "https://retail-sales-demo-mahesh.s3.ap-south-1.amazonaws.com/raw/sample_sales_data.csv"

start_time = datetime.now()

response = requests.get(FILE_URL)
response.raise_for_status()

pdf = pd.read_csv(StringIO(response.text))
record_count = len(pdf)

df_raw = (
    spark.createDataFrame(pdf)
         .withColumn("ingestion_timestamp", current_timestamp())
)

(
    df_raw.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
)

end_time = datetime.now()

logger.info(f"✓ Bronze table written: {BRONZE_TABLE}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Layer (Delta Lake) and Log Ingestion Metrics

# COMMAND ----------

# =====================================
# RETURN METRICS TO ORCHESTRATION
# =====================================

duration_seconds = (end_time - start_time).total_seconds()
total_size_mb = len(response.content) / (1024 * 1024)

metrics = {
    "status": "SUCCESS",
    "bronze_table": BRONZE_TABLE,
    "source_path": source_path,
    "load_type": load_type,
    "file_format": file_format,
    "files_processed": 1,
    "records_ingested": record_count,
    "total_size_mb": round(total_size_mb, 2),
    "duration_seconds": round(duration_seconds, 2),
    "records_per_second": round(record_count / duration_seconds, 2) if duration_seconds > 0 else 0
}

dbutils.notebook.exit(json.dumps(metrics))

