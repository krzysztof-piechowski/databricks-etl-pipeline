# Databricks notebook source
# MAGIC %md
# MAGIC ## Paths and Resource Locations

# COMMAND ----------

from pyspark.sql.functions import col, split, concat, lit, current_timestamp, sha2, concat_ws, input_file_name, upper
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

# --- PATHS ---
bronze_path     = "abfss://bronze@storageaccpiechk.dfs.core.windows.net/customers/"
silver_path     = "abfss://silver@storageaccpiechk.dfs.core.windows.net/customers/"
checkpoint_path = "abfss://silver@storageaccpiechk.dfs.core.windows.net/_checkpoints/checkpoint_customers/"
schema_location = "abfss://silver@storageaccpiechk.dfs.core.windows.net/_schemas/_schema_customers/"
silver_table    = "databricks_cata.silver.customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

PK = "customer_id"
ZORDER_COL = "customer_id"

# tune for your environment
TARGET_FILE_SIZE = 128 * 1024 * 1024
MIN_PARTS = 10
MAX_PARTS = 128

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.databricks.delta.targetFileSize", str(TARGET_FILE_SIZE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Initialization

# COMMAND ----------

spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_table} (
          customer_id STRING,
          full_name STRING,
          city STRING,
          state STRING,
          email_domain STRING,
          email STRING,
          hash_value STRING,
          last_update_ts TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
        LOCATION '{silver_path}'
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partition Calculation Utility

# COMMAND ----------

def compute_num_partitions_for_batch(microbatch_df: DataFrame):
    num_files = microbatch_df.select("_input_file").distinct().count()
    if num_files == 0:
        return 1
    return min(MAX_PARTS, max(MIN_PARTS, num_files))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Stream (Autoloader)

# COMMAND ----------

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.rescuedDataColumn", "_rescued_data_autoloader")
    .load(bronze_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Transformation

# COMMAND ----------

stream_transformed = (
    bronze_stream
      .withColumn("_input_file", col("_metadata.file_path"))
      .drop("_rescued_data_autoloader")
      .drop("_rescued_data")   
      .withColumn("email_domain", split(col("email"), "@")[1])   
      .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
      .drop("first_name", "last_name")
      .withColumn("hash_value", sha2(concat_ws("||", col("full_name"), col("city"), col("state"), col("email")), 256))
      .withColumn("last_update_ts", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert Function to Delta Table

# COMMAND ----------

def upsert_to_delta(microbatch_df, batch_id):
    if microbatch_df.isEmpty():
        return

    num_parts = compute_num_partitions_for_batch(microbatch_df)
    df_to_write = microbatch_df.repartition(num_parts).drop("_input_file")

    delta_table = DeltaTable.forPath(spark, silver_path)

    try:
        (
        delta_table.alias("t")
        .merge(df_to_write.alias("s"), f"t.{PK} = s.{PK}")
        .whenMatchedUpdate(set={
            "full_name": col("s.full_name"),
            "city": col("s.city"),
            "state": col("s.state"),
            "email_domain": col("s.email_domain"),
            "email": col("s.email"),
            "last_update_ts": col("s.last_update_ts"),
            "hash_value": col("s.hash_value")
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    except AnalysisException as e:
        print(f"Write failed: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Streaming Upsert

# COMMAND ----------

query = (
    stream_transformed.writeStream
    .option("checkpointLocation", checkpoint_path)
    .foreachBatch(upsert_to_delta)
    .trigger(once=True)   
    .start()
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Write Optimization

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY ({ZORDER_COL})")

# COMMAND ----------

