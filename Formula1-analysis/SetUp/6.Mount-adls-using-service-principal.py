# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC ## Steps
# MAGIC 1. Get client_id, tenent_id, and client_secret from key valt
# MAGIC 2. Set spark config with App/Client id, Directory /Tenant Id & Secret
# MAGIC 3. Call file system utility mount to mount storage
# MAGIC 4. Explore other file system utilities related to mount(list all mounts , unmont)

# COMMAND ----------

client_id=dbutils.secrets.get(scope='Formila11986Scope',key='Formula1ClientID')
tenant_id=dbutils.secrets.get(scope='Formila11986Scope', key='Formula1-tenentID')
clent_secret=dbutils.secrets.get(scope='Formila11986Scope',key='Formula1-ClientSecret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": clent_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl1986.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dl/demo')
