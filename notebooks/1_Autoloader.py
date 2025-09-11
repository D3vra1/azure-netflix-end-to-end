# Databricks notebook source
# DBTITLE 1,olo
# MAGIC %md
# MAGIC # Incremental Data LOading Using Auto Loader

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA netflix_catalog_devv.net_schema;

# COMMAND ----------

checkpoint_location = "abfss://silver@netflixprojectdldevraj.dfs.core.windows.net/checkpoints"

# COMMAND ----------

df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_location)\
  .load("abfss://raw@netflixprojectdldevraj.dfs.core.windows.net")

# COMMAND ----------

display(df)

# COMMAND ----------

 df.writeStream\
  .option("checkpointLocation", checkpoint_location)\
  .trigger(processingTime='10 seconds')\
  .start("abfss://bronze@netflixprojectdldevraj.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

