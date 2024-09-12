# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a schema that we want and then read the csv file using that schema

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StringType, IntegerType, DateType, TimestampType, StructType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/races.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ####### Now adding two new columns (ingestion time and timestamp)  also selecting the requored column and renaming them in the same node

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # this dint work as we first selected the columns and then worked on the columns that were not selected
# MAGIC from pyspark.sql.functions import col, to_timestamp, concat, lit, current_timestamp
# MAGIC races_selected_df = races_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id")) \
# MAGIC     .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
# MAGIC         .withColumn('ingestion_date', current_timestamp())
# MAGIC display(races_selected_df)
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat, lit
races_selected_df = races_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')).select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("race_timestamp")).withColumn("data_source", lit(v_data_source))


# COMMAND ----------

races_final_df = add_ingestion_date(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing the data frame back into the file system

# COMMAND ----------

races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1dl1986/processed/races")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
