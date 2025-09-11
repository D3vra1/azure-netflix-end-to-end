# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Data Transformation

# COMMAND ----------

df = spark.read.format("delta")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load("abfss://bronze@netflixprojectdldevraj.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.fillna({"duration_minutes": 0, "duration_seasons": 1})

# COMMAND ----------

df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))\
            .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("Short_title",split(col("title"), ":").getItem(0))
df.display()

# COMMAND ----------

df = df.withColumn("rating",split(col("rating"), "-").getItem(0))
df.display()

# COMMAND ----------

# reload the silver dataset (full data)
df_full = spark.read.format("delta").load("abfss://bronze@netflixprojectdldevraj.dfs.core.windows.net/netflix_titles/")

print("Full row count:", df_full.count())

# now group by type
from pyspark.sql.functions import col, count, trim

df_grouped = (df_full
              .withColumn("type", trim(col("type")))
              .groupBy("type")
              .agg(count("*").alias("total_count")))

df_grouped.show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

# Clean whitespaces and filter only valid values
df = df.withColumn("type", trim(col("type"))) \
       .filter(col("type").isin("Movie", "TV Show"))

df = df.withColumn(
    "type_flag",
    when(col("type") == "Movie", 1)
    .when(col("type") == "TV Show", 2)
)

# Now group
df_grouped = df.groupBy("type", "type_flag").agg(count("*").alias("total_count"))

df_grouped.display()

# COMMAND ----------

from pyspark.sql.window import Window
df = df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

df.createOrReplaceGlobalTempView("global_view")

# COMMAND ----------

df = spark.sql("""
               select * from global_temp.global_view
               
               """)

# COMMAND ----------

df = spark.sql("""select * from temp_view""")
df.display()

# COMMAND ----------

from pyspark.sql.functions import count, col, trim

# reload full data
df_full = spark.read.format("delta").load("abfss://bronze@netflixprojectdldevraj.dfs.core.windows.net/netflix_titles/")

print("Full row count:", df_full.count())  # should be thousands, not 2

# clean 'type' column (remove spaces)
df_full = df_full.withColumn("type", trim(col("type")))

# group by type
df_grouped = df_full.groupBy("type").agg(count("*").alias("total_count"))
df_grouped.display()

# COMMAND ----------

df = df.groupBy("type").agg(count("*").alias("total_count"))
df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path","abfss://silver@netflixprojectdldevraj.dfs.core.windows.net/netflix_titles").save()

# COMMAND ----------

