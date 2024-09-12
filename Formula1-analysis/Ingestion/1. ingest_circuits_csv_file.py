# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read csv file using dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC  #### circuits_df=spark.read.option("header", True).csv('/mnt/formula1dl1986/raw/circuits.csv')
# MAGIC
# MAGIC  #### circuits_df=spark.read.option("header", True).option("inferSchema", True).csv('dbfs:/mnt/formula1dl1986/raw/circuits.csv')
# MAGIC  #### Above code is used to infer schema based on the data. Its not recomended in production

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuit_schema= StructType(fields=[StructField("circuitId", IntegerType(), False),
                                   StructField("circuitRef", StringType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("location", StringType(), True),
                                   StructField("country", StringType(), True),
                                   StructField("lat", DoubleType(), True),
                                   StructField("lng", DoubleType(), True),
                                   StructField("alt", IntegerType(), True),
                                   StructField("url", StringType(), True)])

# COMMAND ----------

# now we read the data with the schema created
circuits_df=spark.read.option("header", True).schema(circuit_schema).csv(f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select only required columns

# COMMAND ----------

circuits_selected_df= circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt" )

# other ways of selecting the columns
# circuits_selected_df= circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, "lng", circuits_df.alt )
# circuits_selected_df= circuits_df.select(circuits_df["circuitId"], circuits_df.["circuitRef"], circuits_df.["name"], circuits_df.["location"], circuits_df.["country"], circuits_df.["lat"], circyuits_df.["lng"], circuits_df.["alt"] )
# the other way will be using the col method as below
# -------------------------------------------
# from pyspark.sql.functions import col
# circuits_selected_df= circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming the column headers
# MAGIC #### One way will be to use the alias function along with the col fuction
# MAGIC #### other method as below

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumn("data_source", lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding ingetion date to dataframe

# COMMAND ----------

circuits_final_df= add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the dataframe back into the file system this time in Processed folder

# COMMAND ----------

#circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")
# initially we wrote it as a parquet file. Now we are saving it as a table
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

df =  spark.read.parquet("/mnt/formula1dl1986/processed/circuits")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
