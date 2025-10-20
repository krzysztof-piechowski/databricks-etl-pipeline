# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.table("databricks_cata.bronze.regions")

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
  .mode("overwrite")\
  .save("abfss://silver@storageaccpiechk.dfs.core.windows.net/regions")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.regions
# MAGIC using delta
# MAGIC location "abfss://silver@storageaccpiechk.dfs.core.windows.net/regions"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.regions

# COMMAND ----------

