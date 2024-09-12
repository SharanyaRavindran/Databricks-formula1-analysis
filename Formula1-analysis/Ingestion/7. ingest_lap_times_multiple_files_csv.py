# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap times from multipe csv files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField, StringType, IntegerType, StructType

# COMMAND ----------

# creat the schema
lap_time_schema = StructType(fields=(StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("lap", IntegerType(), False),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)))

# COMMAND ----------

# read the file with the schema
lap_time_df = spark.read.schema(lap_time_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

#rename and add new columns
lap_time_final_df = lap_time_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

lap_time_final_df = add_ingestion_date(lap_time_final_df)

# COMMAND ----------

# writing the df into file system
lap_time_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl1986/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")
