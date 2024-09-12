# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results file with partition

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StringType, IntegerType, StructType, DateType, FloatType, DoubleType

# COMMAND ----------

# creating the schema
results_schema = StructType(fields=(StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("constructorId", IntegerType(), False),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), False),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), False),
                                    StructField("positionOrder", IntegerType(), False),
                                    StructField("points", FloatType(), False),
                                    StructField("laps", IntegerType(), False),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", DoubleType(), True),
                                    StructField("statusId", IntegerType(), False),
                                    ))

# COMMAND ----------

# reading results.json with schema
result_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# renaming columns and adding new ones
result_rename_df = result_df.withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("positionText", "position_text").withColumnRenamed("positionOrder", "position_order").withColumnRenamed("fastestLap", "fastest_lap")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

result_rename_df = result_rename_df.withColumnRenamed("fastestLapTime", "fastest_lap_time").withColumnRenamed("fastestLapSpeed", "fastest_lap_speed").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

result_rename_df= add_ingestion_date(result_rename_df)

# COMMAND ----------

result_final_df= result_rename_df.drop("statusId")

# COMMAND ----------

result_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

df= spark.read.parquet("/mnt/formula1dl1986/processed/results")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
