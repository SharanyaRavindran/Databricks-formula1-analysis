# Databricks notebook source
# MAGIC %md
# MAGIC ### Read the json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# creating schema using another style (DDL Formatted style)
constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# Reading into a data frame 
constructor_df= spark.read.schema(constructor_schema).json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

constructor_dropped_df= constructor_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# writing the df into file system
constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1dl1986/processed/constructors")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
