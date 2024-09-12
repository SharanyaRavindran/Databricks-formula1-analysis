# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col
constructor_standing_df = race_results_df.groupBy("race_year", "team")\
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standing_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc
windows_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
constructor_standing_df = constructor_standing_df.withColumn("rank", rank().over(windows_spec))

# COMMAND ----------

display(constructor_standing_df.filter("race_year =  2020"))

# COMMAND ----------

constructor_standing_df.write.format("parquet").saveAsTable("f1_presentation.constructor_standing")
