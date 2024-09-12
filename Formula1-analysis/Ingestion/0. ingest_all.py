# Databricks notebook source
v_result = dbutils.notebook.run("1. ingest_circuits_csv_file", 0, {"p_data_source": "Some online API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2. ingest_races_csv_file", 0, {"p_data_source": "Some online API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. ingest_constructors_json_file", 0, {"p_data_source": "Some online API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. ingest_drivers_json_with_inner_json_obj", 0, {"p_data_source": "Some online API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5. ingest_results_json_with_partition", 0, {"p_data_source": "Some online API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6. ingest_pit_stop_muli_line_json", 0, {"p_data_source": "Some online API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7. ingest_lap_times_multiple_files_csv", 0, {"p_data_source": "Some online API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8. ingest_qualifying_multiple_multiLine_json_files", 0, {"p_data_source": "Some online API"})

# COMMAND ----------

v_result
