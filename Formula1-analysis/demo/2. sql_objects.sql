-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### creating database
-- MAGIC

-- COMMAND ----------

create database if not exists demo; 

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### if we want to switch to a databade 
-- MAGIC USE db_name

-- COMMAND ----------


