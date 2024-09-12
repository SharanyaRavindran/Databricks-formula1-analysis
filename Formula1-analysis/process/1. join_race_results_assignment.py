# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuit_df = spark.read.parquet(f"{processed_folder_path}/circuits")
circuit_df = circuit_df.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")
races_df = races_df.withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

races_circuits_df = races_df.alias("races").join(circuit_df, races_df.circuit_id == circuit_df.circuit_id).select("races.circuit_id","race_year", "race_name", "race_date", "circuit_location", "race_id")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time", "race_time")

# COMMAND ----------

driver_df = spark.read.parquet(f"{processed_folder_path}/drivers")
driver_df = driver_df.withColumnRenamed("name", "driver_name").withColumnRenamed("number", "diver_number").withColumnRenamed("nationality","diver_nationality")


# COMMAND ----------

result_driver_df = results_df.join(driver_df, results_df.driver_id == driver_df.driver_id).select("driver_name", "diver_number", "diver_nationality", "grid", "fastest_lap", "race_time", "points", "race_id", "constructor_id", "position")

# COMMAND ----------

results_driver_races_df = result_driver_df.join(races_circuits_df, result_driver_df.race_id == races_circuits_df.race_id)

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name" , "team")

# COMMAND ----------

fianl_race_reault = results_driver_races_df.join(constructor_df, results_driver_races_df.constructor_id == constructor_df.constructor_id).select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "diver_number", "diver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
fianl_race_reault = fianl_race_reault.withColumn("created_date", current_timestamp())

# COMMAND ----------

display(fianl_race_reault)

# COMMAND ----------

display(fianl_race_reault.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(fianl_race_reault.points.desc()))

# COMMAND ----------

fianl_race_reault.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
