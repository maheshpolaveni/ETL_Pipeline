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

import json
import time
import uuid
import logging
from datetime import datetime


# COMMAND ----------

# MAGIC %md
# MAGIC ## Logger Configuration

# COMMAND ----------

# ---------------------------
# Logger
# ---------------------------
logger = logging.getLogger("ETL_ORCHESTRATION")
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

# ---------------------------
# Run metadata
# ---------------------------
pipeline_run_id = f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
pipeline_start_time = time.time()

logger.info(f"Pipeline started | run_id={pipeline_run_id}")


# COMMAND ----------

# =====================================
# Helper: Run notebook safely
# =====================================

def run_notebook(step_name, notebook_path, params=None, timeout=0):
    logger.info(f"Starting step: {step_name}")

    try:
        raw_output = dbutils.notebook.run(
            notebook_path,
            timeout,
            params or {}
        )

        result = json.loads(raw_output)

        if result.get("status") != "SUCCESS":
            raise Exception(f"{step_name} FAILED | payload={result}")

        logger.info(f"{step_name} completed successfully")

        return {
            "status": "SUCCESS",
            "duration_seconds": result.get("duration_seconds", 0)
        }

    except Exception as e:
        logger.error(f"{step_name} failed: {e}")
        return {
            "status": "FAILED",
            "error": str(e)
        }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution Tracking

# COMMAND ----------

# =====================================
# Execute Pipeline
# =====================================

pipeline_results = {
    "steps": {},
    "errors": [],
    "warnings": []
}

# ---------------------------
# INGESTION
# ---------------------------
pipeline_results["steps"]["ingestion"] = run_notebook(
    step_name="Ingestion",
    notebook_path="../01_ingestion/ingest_sales_data",
    params={
        "source_path": "",
        "target_date": "",
        "load_type": "full",
        "file_format": "csv"
    }
)

# Stop pipeline if ingestion failed
if pipeline_results["steps"]["ingestion"]["status"] == "FAILED":
    pipeline_results["errors"].append("Ingestion FAILED")
else:
    # ---------------------------
    # TRANSFORMATION
    # ---------------------------
    pipeline_results["steps"]["transformation"] = run_notebook(
        step_name="Transformation",
        notebook_path="../02_transformation/transform_sales_data"
    )

    if pipeline_results["steps"]["transformation"]["status"] == "FAILED":
        pipeline_results["errors"].append("Transformation FAILED")
    else:
        # ---------------------------
        # QUALITY CHECKS
        # ---------------------------
        pipeline_results["steps"]["quality_checks"] = run_notebook(
            step_name="Quality Checks",
            notebook_path="../03_quality_checks/data_quality_validation",
            params={
                "silver_table": "retail.sales_silver",
                "fail_on_critical": "false"
            }
        )

        if pipeline_results["steps"]["quality_checks"]["status"] == "FAILED":
            pipeline_results["errors"].append("Quality checks FAILED")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Status

# COMMAND ----------

# =====================================
# FINAL PIPELINE EXIT
# =====================================

pipeline_end_time = time.time()
pipeline_duration = round(pipeline_end_time - pipeline_start_time, 2)

# Determine final status
final_status = "FAILED" if pipeline_results["errors"] else "SUCCESS"

exit_payload = {
    "status": final_status,
    "run_id": pipeline_run_id,
    "duration_seconds": pipeline_duration,
    "steps": pipeline_results["steps"],
    "errors": pipeline_results["errors"],
    "warnings": pipeline_results["warnings"]
}

if final_status == "SUCCESS":
    logger.info("✓ ETL Pipeline completed successfully")
else:
    logger.error(f"✗ ETL Pipeline FAILED | errors={pipeline_results['errors']}")

dbutils.notebook.exit(json.dumps(exit_payload))

