-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### similar to the views on data frames the views on the tables also have to types. temp view and global temp view. Have same features as the views on the dataframe. only diffrece will be there is one more view called the permanenet view. As the name suggests it is permanant and is saved in the hive meta store. Stays even if the cluster is restarted. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. creating temporary view

-- COMMAND ----------

create temporary view v_race_result
as
select * from demo.race_result_ext_py
where race_year = 2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. creating global temp view
-- MAGIC    * to view the global temp view we have to use "global_temp.view_name"
-- MAGIC    * as the global view is saved in global_temp db

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_result
AS
SELECT * FROM demo.race_result_ext_py
WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_result

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. creating permanent view
-- MAGIC   * when naming the view better to prefix it with the db where we need it to be created. otherwise it will be created in the default db

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_result
AS
SELECT * FROM demo.race_result_ext_py
WHERE race_year = 2000

-- COMMAND ----------

show tables in demo
