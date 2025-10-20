# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
import time, uuid
import traceback

# COMMAND ----------

checkpoint_log_table_path = "abfss://gold@storageaccpiechk.dfs.core.windows.net/_checkpoints/"
checkpoint_log_table = "databricks_cata.gold.checkpoint_log"

batch_log_table_path = "abfss://gold@storageaccpiechk.dfs.core.windows.net/_batches/"
batch_log_table = "databricks_cata.gold.batch_log"

# COMMAND ----------

# Ensure checkpoint table exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {checkpoint_log_table} (
  table_name STRING,
  last_processed_version LONG,
  last_processed_ts TIMESTAMP
)
USING DELTA
LOCATION '{checkpoint_log_table_path}'
""")

# COMMAND ----------

# Ensure batches table exists for idempotency and auditing
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {batch_log_table} (
  batch_id STRING,
  table_name STRING,
  starting_version LONG,
  ending_version LONG,
  row_count LONG,
  status STRING,
  started_ts TIMESTAMP,
  finished_ts TIMESTAMP,
  error_msg STRING
)
USING DELTA
LOCATION '{batch_log_table_path}'
""")