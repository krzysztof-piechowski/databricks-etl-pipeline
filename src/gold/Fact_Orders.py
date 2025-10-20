# Databricks notebook source
# MAGIC %md
# MAGIC ## Paths and Resource Locations

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import time, uuid, smtplib, traceback

# --- PATHS ---
silver_path = "abfss://silver@storageaccpiechk.dfs.core.windows.net/orders/"
gold_fact_path = "abfss://gold@storageaccpiechk.dfs.core.windows.net/FactOrders/"
gold_fact_table = "databricks_cata.gold.FactOrders"

checkpoint_log_table = "databricks_cata.gold.checkpoint_log"
checkpoint_table_name = 'FactOrders'

batch_log_table = "databricks_cata.gold.batch_log"

missing_dims_path = "abfss://gold@storageaccpiechk.dfs.core.windows.net/FactOrders_MissingDims/"
staging_root = "abfss://gold@storageaccpiechk.dfs.core.windows.net/FactOrders_Staging/"

# Gold dim table names
dim_products_table = "databricks_cata.gold.DimProducts"
dim_customers_table = "databricks_cata.gold.DimCustomers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

ZORDER_COLS = ["customer_sk", "product_sk"]
PARTITION_COLS = ["year", "month"]

# tune for your environment
TARGET_FILE_SIZE = 256 * 1024 * 1024  
BROADCAST_THRESHOLD = 100 * 1024 * 1024  

MAX_RETRIES = 2
RETRY_BACKOFF_SEC = 10

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.databricks.delta.targetFileSize", str(TARGET_FILE_SIZE))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",str(BROADCAST_THRESHOLD)) 

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

def generate_batch_id(prefix="FactOrders", attempt_num=None):
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

partition_clause = ", ".join(PARTITION_COLS)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_fact_table} (
  order_id STRING,
  order_date DATE,
  year INT,
  month INT,
  customer_sk BIGINT,
  product_sk BIGINT,
  quantity DOUBLE,
  total_amount DOUBLE,
  load_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY ({partition_clause})
LOCATION '{gold_fact_path}'
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
        print(f"Attempt {attempt} starting. Batch id: {batch_id}")

        last_version = get_last_processed_version()
        current_version = get_current_silver_version()

        print(f"Last processed version: {last_version}")
        print(f"Current silver version: {current_version}")

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
            print("This version range was already processed successfully. Exiting.")
            break

        # Create batch log
        log_batch_start(batch_id, starting_version, ending_version)

        print(f"Reading CDF starting from version {starting_version} to {ending_version}")

        # Read CDF from silver
        cdf_df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", str(starting_version))
            .option("endingVersion", str(ending_version))
            .load(silver_path)
        )

        # Filter inserts
        inserts_df = cdf_df.filter(F.col("_change_type") == 'insert')
        if inserts_df.limit(1).count() == 0:

            print("No insert changes in CDF range. Updating checkpoint and exiting.")
            upsert_checkpoint(ending_version)
            # mark batch as SUCCESS with 0 rows
            log_batch_end(batch_id, 0, status='SUCCESS')
            break

        # Join with current dimension tables to get surrogate keys
        dim_products = spark.table(dim_products_table).filter("is_current = true").select("product_id", "product_sk")
        dim_customers = spark.table(dim_customers_table).filter("is_current = true").select("customer_id", "customer_sk")

        transformed = (
            inserts_df
             .withColumn("order_date", F.to_date(F.col("order_date")))
             .withColumn("year", F.year(F.col("order_date")))
             .withColumn("month", F.month(F.col("order_date")))
             .join(F.broadcast(dim_products), on=["product_id"], how="left")
             .join(F.broadcast(dim_customers), on=["customer_id"], how="left")
             .select(
                F.col("order_id").alias("order_id"),
                F.col("order_date"),
                F.col("year"),
                F.col("month"),
                F.col("customer_sk"),
                F.col("product_sk"),
                F.col("quantity"),
                F.col("total_amount"),
                F.current_timestamp().alias("load_ts")
            )
            .withColumn("batch_id", F.lit(batch_id))
        )

        # Handle missing surrogate keys: write unmatched to missing_dims_table
        missing_dims = transformed.filter(F.col("customer_sk").isNull() | F.col("product_sk").isNull())
        missing_count = missing_dims.count()
        if missing_count > 0:
            print(f"WARNING: {missing_count} rows with missing surrogate keys. Writing to staging for manual review.")
            missing_dims.write.format("delta").mode("append").save(missing_dims_path)

        # Idempotent publish: write to staging and MERGE into gold fact table
        staging_path = staging_root + f"batch_{batch_id}/"
        transformed.write.format("delta").mode("overwrite").save(staging_path)

        # MERGE: insert only when order_id not exists (idempotent)
        delta_fact = DeltaTable.forPath(spark, gold_fact_path)
        staging_df = spark.read.format("delta").load(staging_path)

        appended_count = staging_df.count()
        print(f"Batch contains {appended_count} records to process.")       

        # Broadcast the small staging table so Spark can push down year/month filters
        # and leverage partition pruning on the fact table (partitioned by year, month).

        # Can be turned off if the staging table is too large.
        delta_fact.alias("t").merge(
            #staging_df.alias("s"),
            F.broadcast(staging_df).alias("s"),
            "t.order_id = s.order_id AND t.year = s.year AND t.month = s.month"
        ).whenNotMatchedInsertAll().execute()

        # Update checkpoint and batch log
        upsert_checkpoint(ending_version)
        log_batch_end(batch_id, appended_count, status='SUCCESS')

        # Optimize gold fact table
        print("Running OPTIMIZE + ZORDER on FactOrders (this may be slow depending on volume).")
        spark.sql(f"OPTIMIZE delta.`{gold_fact_path}` ZORDER BY ({', '.join(ZORDER_COLS)})")

        print("Batch processed successfully.")
        break

    except Exception as e:
        
        # Handle errors
        error_trace = traceback.format_exc()  
        print(f"Error during batch {batch_id}: {e}")
        print(error_trace)  
        
        log_batch_end(batch_id, 0, status='FAILED', error_msg=error_trace)
        if attempt < MAX_RETRIES:
            sleep_time = RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
            print(f"Retrying after {sleep_time} seconds...")
            time.sleep(sleep_time)
            continue
        else:
            # Exhausted retries, re-raise
            raise

print("Done.")


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

# COMMAND ----------

