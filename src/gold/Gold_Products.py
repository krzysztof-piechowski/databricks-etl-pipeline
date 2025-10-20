# Databricks notebook source
# MAGIC %md
# MAGIC ## Paths and Resource Locations

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
import time, uuid
import traceback

# --- PATHS ---
silver_path = "abfss://silver@storageaccpiechk.dfs.core.windows.net/products/"
gold_path  = "abfss://gold@storageaccpiechk.dfs.core.windows.net/DimProducts/"
gold_dim_table = "databricks_cata.gold.DimProducts"

checkpoint_log_table = "databricks_cata.gold.checkpoint_log"
checkpoint_table_name = "DimProducts"

batch_log_table = "databricks_cata.gold.batch_log"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

ZORDER_COLS = "product_sk"

# tune for your environment
TARGET_FILE_SIZE = 128 * 1024 * 1024
MAX_RETRIES = 2
RETRY_BACKOFF_SEC = 10

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.databricks.delta.targetFileSize", str(TARGET_FILE_SIZE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Version & Checkpoint Management Functions

# COMMAND ----------

def get_current_silver_version():
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{silver_path}`")
    maxv = hist.select(F.max(F.col('version'))).collect()[0][0]
    return maxv

def get_last_processed_version():
    df = spark.sql(f"SELECT last_processed_version FROM {checkpoint_log_table} WHERE table_name = '{checkpoint_table_name}'")
    if df.count() == 0:
        return None
    return df.collect()[0][0]

def upsert_checkpoint(version):
    spark.sql(f"""
    MERGE INTO {checkpoint_log_table} AS c
    USING (SELECT '{checkpoint_table_name}' AS table_name, {int(version)} AS last_processed_version, current_timestamp() AS last_processed_ts) AS s
    ON c.table_name = s.table_name
    WHEN MATCHED THEN UPDATE SET c.last_processed_version = s.last_processed_version, c.last_processed_ts = s.last_processed_ts
    WHEN NOT MATCHED THEN INSERT (table_name, last_processed_version, last_processed_ts) VALUES (s.table_name, s.last_processed_version, s.last_processed_ts)
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Logging & ID Utilities

# COMMAND ----------

def log_batch_start(batch_id, starting_version, ending_version):
    spark.sql(f"INSERT INTO {batch_log_table} (batch_id, table_name, starting_version, ending_version, row_count, status, started_ts) VALUES ('{batch_id}', '{checkpoint_table_name}', {starting_version}, {ending_version}, 0, 'RUNNING', current_timestamp())")

def log_batch_end(batch_id, row_count, status='SUCCESS', error_msg=None):
    err = f"'{error_msg.replace("'","\''")}'" if error_msg else 'NULL'
    spark.sql(f"UPDATE {batch_log_table} SET row_count = {int(row_count)}, status = '{status}', finished_ts = current_timestamp(), error_msg = {err} WHERE batch_id = '{batch_id}'")

def generate_batch_id(prefix="DimProducts", attempt_num=None):
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    if attempt_num is not None:
        return f"{prefix}_{timestamp}_try{attempt_num}"
    else:
        return f"{prefix}_{timestamp}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Creation & Initialization

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_dim_table} (
    product_sk BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING,
    price DOUBLE,
    hash_value STRING,
    last_update_ts TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
)
USING DELTA
LOCATION '{gold_path}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Loop with Retry and Error Handling

# COMMAND ----------

attempt = 0
while attempt < MAX_RETRIES:
    attempt += 1
    batch_id = generate_batch_id(prefix=checkpoint_table_name, attempt_num=attempt)

    try:
        print(f"Attempt {attempt} - Batch ID: {batch_id}")

        last_version = get_last_processed_version()
        current_version = get_current_silver_version()

        print(f"Last processed Silver version: {last_version}")
        print(f"Current Silver version: {current_version}")

        if last_version is not None and last_version >= current_version:
            print("No new changes to process.")
            break

        starting_version = 0 if last_version is None else int(last_version) + 1
        ending_version = current_version

        # Skip if already succeeded for this range
        existing = spark.sql(f"""
            SELECT batch_id FROM {batch_log_table}
            WHERE table_name = '{checkpoint_table_name}'
              AND starting_version = {starting_version}
              AND ending_version = {ending_version}
              AND status = 'SUCCESS'
        """)
        if existing.count() > 0:
            print("This version range already processed. Skipping.")
            break

        # Create batch log
        log_batch_start(batch_id, starting_version, ending_version)

        print(f"Reading CDF from version {starting_version} to {ending_version} ...")

        # Read CDF from silver
        cdf_df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", str(starting_version))
            .option("endingVersion", str(ending_version))
            .load(silver_path)
        )

        # Filter inserts and post-images
        changed_df = cdf_df.filter(F.col("_change_type").isin("insert", "update_postimage"))
        if changed_df.limit(1).count() == 0:
            print("No changes detected in Silver CDF.")
            upsert_checkpoint(ending_version)
            log_batch_end(batch_id, 0, status='SUCCESS')
            break

        # Prepare SCD2 records
        new_records = (
            changed_df
            .select(
                "product_id", "product_name", "category", "brand",
                "price", "hash_value", "last_update_ts"
            )
            .withColumn("valid_from", F.current_timestamp())
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )

        delta_gold = DeltaTable.forPath(spark, gold_path)

        # Expire old versions
        delta_gold.alias("g") \
            .merge(
                new_records.select("product_id").alias("s"),
                "g.product_id = s.product_id AND g.is_current = true"
            ) \
            .whenMatchedUpdate(set={
                "valid_to": F.current_timestamp(),
                "is_current": F.lit(False)
            }) \
            .execute()

        # Append new versions
        new_records.write.format("delta").mode("append").save(gold_path)

        updated_count = new_records.count()
        print(f"Upserted {updated_count} records to Gold DimProducts")

        # Update checkpoint and batch log
        upsert_checkpoint(ending_version)
        log_batch_end(batch_id, updated_count, status='SUCCESS')

        # Optimize gold table
        print("Running OPTIMIZE + ZORDER ...")
        spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY {ZORDER_COLS}")

        print("Batch completed successfully.")
        break

    except Exception as e:

        # Handle errors
        error_trace = traceback.format_exc()
        print(f"Error during batch {batch_id}: {e}")
        print(error_trace)
        
        log_batch_end(batch_id, 0, status='FAILED', error_msg=error_trace)

        if attempt < MAX_RETRIES:
            sleep_time = RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
            print(f"Retrying after {sleep_time} sec...")
            time.sleep(sleep_time)
        else:
            raise

print("DimProducts pipeline finished.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Checkpoint Log

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from databricks_cata.gold.checkpoint_log

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Batch Log

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from databricks_cata.gold.batch_log