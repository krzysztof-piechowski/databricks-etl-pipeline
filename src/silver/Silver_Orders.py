# Databricks notebook source
# MAGIC %md
# MAGIC ## Paths and Resource Locations

# COMMAND ----------

from pyspark.sql.functions import col, split, concat, lit, current_timestamp, sha2, concat_ws, input_file_name, dayofweek, dayofmonth, to_date, year, month
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

bronze_path     = "abfss://bronze@storageaccpiechk.dfs.core.windows.net/orders/"
silver_path     = "abfss://silver@storageaccpiechk.dfs.core.windows.net/orders/"
checkpoint_path = "abfss://silver@storageaccpiechk.dfs.core.windows.net/_checkpoints/checkpoint_orders/"
schema_location = "abfss://silver@storageaccpiechk.dfs.core.windows.net/_schemas/_schema_orders/"
silver_table    = "databricks_cata.silver.orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

PK = "order_id"
ZORDER_COL = ["customer_id", "product_id"]
PARTITION_COLS = ["year", "month"]

# tune for your environment
TARGET_FILE_SIZE = 256 * 1024 * 1024
NUM_OF_PARTITIONS = 128 

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.databricks.delta.targetFileSize", str(TARGET_FILE_SIZE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Initialization

# COMMAND ----------

partition_clause = ", ".join(PARTITION_COLS)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity DOUBLE,
        total_amount DOUBLE,
        order_date DATE,
        year INT,
        month INT,
        day_of_week INT,
        day_of_month INT,
        hash_value STRING,
        last_update_ts TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY ({partition_clause})
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    LOCATION '{silver_path}'
""")

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
      .withColumn("order_date", to_date(col("order_date")))
      .withColumn("year", year(col("order_date")))
      .withColumn("month", month(col("order_date")))
      .withColumn("day_of_week", dayofweek(col("order_date")))
      .withColumn("day_of_month", dayofmonth(col("order_date")))
      .withColumn("hash_value", sha2(concat_ws("||", col("customer_id"), col("product_id"), col("quantity"), col("total_amount")), 256))
      .withColumn("last_update_ts", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert Function to Delta Table

# COMMAND ----------

def upsert_to_delta(microbatch_df, batch_id):
    if microbatch_df.isEmpty():
        return

    df_to_write = microbatch_df.repartition(NUM_OF_PARTITIONS)
    delta_table = DeltaTable.forPath(spark, silver_path)

    try:
        (
        delta_table.alias("t")
        .merge(df_to_write.alias("s"), f"t.{PK} = s.{PK}")
        .whenMatchedUpdate(set={
            "customer_id": col("s.customer_id"),
            "product_id": col("s.product_id"),
            "quantity": col("s.quantity"),
            "total_amount": col("s.total_amount"),
            "order_date": col("s.order_date"),
            "year": col("s.year"),
            "month": col("s.month"),
            "day_of_week": col("s.day_of_week"),
            "day_of_month": col("s.day_of_month"),
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

spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY ({', '.join(ZORDER_COL)})")

# COMMAND ----------

