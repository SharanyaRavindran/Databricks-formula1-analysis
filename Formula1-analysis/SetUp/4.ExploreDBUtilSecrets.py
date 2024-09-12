# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='Formila11986Scope')

# COMMAND ----------

dbutils.secrets.get(scope='Formila11986Scope', key='Formula1dl1986-account-key')
