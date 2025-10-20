# Databricks notebook source
# MAGIC %md
# MAGIC ## Paths and Resource Locations

# COMMAND ----------

from pyspark.sql.functions import col, split, concat, lit, current_timestamp, sha2, concat_ws, input_file_name, upper
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

# --- PATHS ---
bronze_path     = "abfss://bronze@storageaccpiechk.dfs.core.windows.net/products/"
silver_path     = "abfss://silver@storageaccpiechk.dfs.core.windows.net/products/"
checkpoint_path = "abfss://silver@storageaccpiechk.dfs.core.windows.net/_checkpoints/checkpoint_products/"
schema_location = "abfss://silver@storageaccpiechk.dfs.core.windows.net/_schemas/_schema_products/"
silver_table    = "databricks_cata.silver.products"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

PK = "product_id"
ZORDER_COL = "product_id"

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
          product_id STRING,
          product_name STRING,
          category STRING,
          brand STRING,
          price DOUBLE,
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
      .drop("_rescued_data_autoloader", "_rescued_data")
      .withColumn("brand", upper(col("brand"))) 
      .withColumn("hash_value", sha2(concat_ws("||", col("brand"), col("product_name"), col("category"), col("price")), 256))
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
            "product_name": col("s.product_name"),
            "category": col("s.category"),
            "brand": col("s.brand"),
            "price": col("s.price"),
            "hash_value": col("s.hash_value"),
            "last_update_ts": col("s.last_update_ts")
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

