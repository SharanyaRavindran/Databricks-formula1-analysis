-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_result_20202
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DROP TABLE race_result_20202

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## External Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_result_ext_py").saveAsTable("demo.race_result_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_result_ext_py

-- COMMAND ----------


