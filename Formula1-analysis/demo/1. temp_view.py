# Databricks notebook source
# create local temp views for a dataframe and access them with SQL and python
# When accessing with python a dataframe is returned. We can pass in variables for querring 
# a local temporary view is only availabe in that spark session. Its not availabe in another notebook. Also if we detach from the cluster the view is gone.

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("view_race_results")
# race_results_df.createOrReplaceTempView("view_race_results") --> to replece an exixting one

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT (1) FROM view_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_result_view_sql = spark.sql("SELECT * FROM view_race_results WHERE race_year = 2020")

# COMMAND ----------

# A global temp view is availabe within the enitre cluster. 
# it is stored in a database called global_temp, so to access the view use global_temp.{view name}
# Its valid as long is the cluster is up and runing. Even if we detach a notbook and reattach it it still exists.



# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("global_view_race_results")
# race_results_df.createTempView("view_race_results") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.global_view_race_results;
