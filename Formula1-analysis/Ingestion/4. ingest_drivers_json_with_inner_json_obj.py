# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingets drivers json file with inner json object

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

## imports
from pyspark.sql.types import StructField, StringType, IntegerType, StructType, DateType

# COMMAND ----------

# defining inner schema
schema_name = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])

# COMMAND ----------

# defining main schema
schema_driver = StructType(fields=[StructField("driverId", IntegerType(), False),
                                   StructField("driverRef", StringType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("code", StringType(), True),
                                   StructField("name", schema_name),
                                   StructField("dob", DateType(), True),
                                   StructField("nationality", StringType(), True),
                                   StructField("url", StringType(), True)
                                   ])

# COMMAND ----------

# read the file with the schema
driver_df = spark.read.schema(schema_driver).json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# renaming the cloumns and adding new ones
drivers_rename_df=driver_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# droping column
driver_final_df = drivers_rename_df.drop("url")

# COMMAND ----------

driver_final_df = add_ingestion_date(driver_final_df)

# COMMAND ----------

# writing the into file system
driver_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1dl1986/processed/drivers")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
