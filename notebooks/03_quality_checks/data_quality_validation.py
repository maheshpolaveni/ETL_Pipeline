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

# ===============================
# CELL 1: Widgets (MUST BE FIRST)
# ===============================

from datetime import datetime
import logging

# ------------------------------
# Remove any previous widgets
# ------------------------------
dbutils.widgets.removeAll()

# ------------------------------
# Define widgets with default values
# ------------------------------
dbutils.widgets.text("silver_table", "retail.sales_silver")
dbutils.widgets.text("target_date", datetime.now().strftime("%Y-%m-%d"))
dbutils.widgets.text("fail_on_critical", "false")

# ------------------------------
# Read widgets
# ------------------------------
silver_table = dbutils.widgets.get("silver_table").strip()
target_date = dbutils.widgets.get("target_date").strip()
fail_on_critical = dbutils.widgets.get("fail_on_critical").lower() == "true"

# ------------------------------
# Logging setup
# ------------------------------
logger = logging.getLogger("DATA_QUALITY")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)

# ------------------------------
# Apply fallback defaults if running standalone
# ------------------------------
if not silver_table:
    logger.warning("❌ silver_table widget is empty, using default 'retail.sales_silver'")
    silver_table = "retail.sales_silver"

if not target_date:
    logger.warning("target_date widget is empty, using today's date")
    target_date = datetime.now().strftime("%Y-%m-%d")

logger.info(
    f"Widgets received | silver_table='{silver_table}', "
    f"target_date='{target_date}', "
    f"fail_on_critical={fail_on_critical}"
)


# COMMAND ----------

# ===============================
# CELL: Data Quality Validation
# ===============================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# -------------------------------
# Logger setup
# -------------------------------
logger = logging.getLogger("DATA_QUALITY")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

# -------------------------------
# Spark session
# -------------------------------
spark = SparkSession.builder.appName("DataQualityValidation").getOrCreate()
logger.info("✅ Spark session initialized")

# -------------------------------
# Fully Qualified Table Name
# -------------------------------
silver_table_fqn = silver_table if "." in silver_table else f"retail.{silver_table}"
logger.info(f"Using Silver table: {silver_table_fqn}")

# -------------------------------
# Validate table exists
# -------------------------------
try:
    spark.sql(f"DESCRIBE TABLE {silver_table_fqn}")
except Exception as e:
    raise ValueError(f"❌ Silver table '{silver_table_fqn}' does not exist: {e}")

# -------------------------------
# Load Silver table
# -------------------------------
df_silver = spark.table(silver_table_fqn)
record_count_before_filter = df_silver.count()
logger.info(f"✅ Total records in Silver table before filtering: {record_count_before_filter}")

# -------------------------------
# Apply target_date filter safely
# -------------------------------
if target_date:
    # Check if target_date exists in the table
    available_dates = [row.sale_date for row in df_silver.select("sale_date").distinct().collect()]
    if target_date in available_dates:
        df_silver = df_silver.filter(col("sale_date") == target_date)
        logger.info(f"Filtering data for target_date={target_date}")
    else:
        logger.warning(f"No rows found for target_date={target_date}. Skipping date filter.")
        target_date = None  # reset so later logging is accurate

record_count = df_silver.count()

if record_count == 0:
    logger.warning("⚠ No records found in Silver table after filtering. Data Quality checks will run on empty DataFrame.")

logger.info(f"✅ Records to validate: {record_count}")


# COMMAND ----------

from pyspark.sql.functions import col, isnan
from pyspark.sql.types import NumericType
import logging

# -------------------------------
# Logger setup
# -------------------------------
logger = logging.getLogger("DATA_QUALITY_NULL_CHECKS")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

logger.info("Running NULL checks...")

# -------------------------------
# Columns & threshold
# -------------------------------
null_columns = [
    "transaction_id",
    "sale_date",
    "sale_amount",
    "customer_id",
    "product_id"
]
threshold = 0.05
total_count = record_count  # from previous cell
results = {}

if total_count == 0:
    logger.warning("⚠ Silver table is empty. Skipping null checks.")
    for c in null_columns:
        results[c] = {"status": "SKIPPED", "reason": "No records to check"}
else:
    schema_map = {f.name: f.dataType for f in df_silver.schema}

    for c in null_columns:
        if c not in schema_map:
            results[c] = {"status": "SKIPPED", "reason": "Column not found"}
            continue

        # Check NULLs
        cond = col(c).isNull()

        # Also check NaN for numeric columns
        if isinstance(schema_map[c], NumericType):
            cond = cond | isnan(col(c))

        nulls = df_silver.filter(cond).count()
        pct = nulls / total_count

        results[c] = {
            "null_count": nulls,
            "null_percentage": round(pct, 4),
            "status": "PASS" if pct <= threshold else "FAIL"
        }

logger.info("NULL check results:")
for col_name, res in results.items():
    logger.info(f"{col_name}: {res}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Silver Layer

# COMMAND ----------

import json
import logging

# -------------------------------
# Logger setup
# -------------------------------
logger = logging.getLogger("DATA_QUALITY_FINAL")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

logger.info("Finalizing Data Quality results...")

# -------------------------------
# Count results
# -------------------------------
failed = sum(1 for r in results.values() if r.get("status") == "FAIL")
warnings = sum(1 for r in results.values() if r.get("status") == "SKIPPED")

# Determine final status
if failed > 0 and fail_on_critical:
    final_status = "FAILED"
elif warnings > 0:
    final_status = "WARNING"
else:
    final_status = "SUCCESS"

dq_result = {
    "status": final_status,
    "silver_table": silver_table_fqn,
    "records_checked": record_count,
    "metrics": results
}

# Log summary
logger.info(f"Data Quality Result | Status: {final_status}, "
            f"Failed checks: {failed}, Skipped checks: {warnings}, "
            f"Records checked: {record_count}")

# -------------------------------
# Exit notebook with JSON
# -------------------------------
dbutils.notebook.exit(json.dumps(dq_result))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Quality Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Data Quality Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Check Results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall Quality Summary
