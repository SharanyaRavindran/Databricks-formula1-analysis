# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake containers for the Project
# MAGIC

# COMMAND ----------

# declaring a function to mount a container. passing the storage-account-name and container-name as parameters
def mount_adls(storage_account_name,conatiner_name):
    # secrets from key vault
    client_id=dbutils.secrets.get(scope='Formila11986Scope',key='Formula1ClientID')
    tenant_id=dbutils.secrets.get(scope='Formila11986Scope', key='Formula1-tenentID')
    clent_secret=dbutils.secrets.get(scope='Formila11986Scope',key='Formula1-ClientSecret')
    #set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": clent_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    # unmount the mount point if already mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{conatiner_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{conatiner_name}")
    #mount the container 
    dbutils.fs.mount(
        source = f"abfss://{conatiner_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{conatiner_name}",
        extra_configs = configs)
    #display all the mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

# mounting presentation
mount_adls('formula1dl1986', 'presentation')

# COMMAND ----------

dbutils.fs.ls("mnt/formula1dl1986/presentation")

# COMMAND ----------

# mounting processed
mount_adls('formula1dl1986', 'processed')

# COMMAND ----------

# mounting raw
mount_adls('formula1dl1986', 'raw')
