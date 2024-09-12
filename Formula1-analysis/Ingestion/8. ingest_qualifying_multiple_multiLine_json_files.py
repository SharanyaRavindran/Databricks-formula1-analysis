# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest multiple multi line json files 

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

# creating schema
qualifying_schema = StructType(fields=(StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)))

# COMMAND ----------

# reading the file with schema
qualifing_df = spark.read.schema(qualifying_schema).option("multiLine", True).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# rename and add new column
qualifying_final_df = qualifing_df.withColumnRenamed("qualifyId", "qualify_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# wrting the df into file 
qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl1986/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
