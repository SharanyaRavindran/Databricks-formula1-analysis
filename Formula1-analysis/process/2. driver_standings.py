# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.groupBy("race_year","driver_name", "diver_nationality").sum("points").filter("race_year = 2000").show(100)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col
driver_standing_df = race_results_df\
    .groupBy("race_year", "driver_name", "diver_nationality", "team")\
        .agg(sum("points").alias("tatal_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standing_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
windows_spec = Window.partitionBy("race_year").orderBy(desc("tatal_points"),desc("wins"))
final_driver_standinf_df = driver_standing_df.withColumn("rank", rank().over(windows_spec))

# COMMAND ----------

display(final_driver_standinf_df)

# COMMAND ----------

final_driver_standinf_df.write.format("parquet").saveAsTable("f1_presentation.driver_standings")
