# Databricks notebook source
# MAGIC %md
# MAGIC ## Dynamic Capabilities

# COMMAND ----------

dbutils.widgets.text("file_name","")

# COMMAND ----------

source_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
  .option("cloudFiles.schemaLocation",f"abfss://bronze@storageaccpiechk.dfs.core.windows.net/checkpoint_{source_file_name}") \
  .load(f"abfss://source@storageaccpiechk.dfs.core.windows.net/{source_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing

# COMMAND ----------

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation",f"abfss://bronze@storageaccpiechk.dfs.core.windows.net/checkpoint_{source_file_name}")\
    .option("path",f"abfss://bronze@storageaccpiechk.dfs.core.windows.net/{source_file_name}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Check

# COMMAND ----------

# df = spark.read.format("parquet").load(f"abfss://bronze@storageaccpiechk.dfs.core.windows.net/{source_file_name}")
# df.display()

# COMMAND ----------

