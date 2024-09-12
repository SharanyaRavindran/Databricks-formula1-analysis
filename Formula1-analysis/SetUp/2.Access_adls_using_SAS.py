# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using SAS token
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from Deme container
# MAGIC 1. Read data from circuits

# COMMAND ----------

formula1DemoSASToken=dbutils.secrets.get(scope='Formila11986Scope',key='Formula1DemoSAS')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl1986.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl1986.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl1986.dfs.core.windows.net",  formula1DemoSASToken)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1986.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1986.dfs.core.windows.net/circuits.csv"))
