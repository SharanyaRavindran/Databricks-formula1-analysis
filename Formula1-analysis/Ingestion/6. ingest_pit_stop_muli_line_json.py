# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit stop muli line json

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------


from pyspark.sql.types import StructField, StringType, IntegerType, StructType


# COMMAND ----------

# creat the schema
pitstop_schema = StructType(fields=(StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("stop", IntegerType(), False),
                                    StructField("lap", IntegerType(), False),
                                    StructField("time", StringType(), False),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)))

# COMMAND ----------

# read the file with the schema
pitstop_df = spark.read.schema(pitstop_schema).option("multiLine", True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# rename and add new column
pitstop_final_df = pitstop_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

pitstop_final_df = add_ingestion_date(pitstop_final_df)

# COMMAND ----------

pitstop_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stop")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl1986/processed/pit_stop"))

# COMMAND ----------

dbutils.notebook.exit("Success")
