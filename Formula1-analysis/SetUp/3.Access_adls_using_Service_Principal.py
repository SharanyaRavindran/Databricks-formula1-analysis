# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application/Service Principal
# MAGIC 1. Generate a secret/password for the Application
# MAGIC 1. Set Spark Config with App/Client Id, Directory/ Tenant Id and Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributer, to the Data Lake

# COMMAND ----------

client_id=dbutils.secrets.get(scope='Formila11986Scope',key='Formula1ClientID')
tenant_id=dbutils.secrets.get(scope='Formila11986Scope', key='Formula1-tenentID')
clent_secret=dbutils.secrets.get(scope='Formila11986Scope',key='Formula1-ClientSecret')

# COMMAND ----------

client_id="af442f79-45b5-4106-9b41-bcbd9ca73da8"
tenant_id="78898510-af34-497f-a50f-d42e65d2089b"
clent_secret="phi8Q~X2MET6UiSAzlY3QoTAVxAjXq0INxkY5a3V_id"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl1986.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl1986.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl1986.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl1986.dfs.core.windows.net", clent_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl1986.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1986.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1986.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

df = spark.read.csv("abfss://demo@formula1dl1986.dfs.core.windows.net/circuits.csv")
df= df.withColumn("Sha",df._c0)
display(df)

# COMMAND ----------


