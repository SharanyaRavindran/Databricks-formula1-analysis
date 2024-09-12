# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access key
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from Deme container
# MAGIC 1. Read data from circuits

# COMMAND ----------

formula1DlAccKey=dbutils.secrets.get(scope='Formila11986Scope', key='Formula1dl1986-account-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dl1986.dfs.core.windows.net",formula1DlAccKey)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1986.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1986.dfs.core.windows.net/circuits.csv"))
