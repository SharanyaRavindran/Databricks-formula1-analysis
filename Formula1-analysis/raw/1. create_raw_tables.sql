-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING 
)
USING csv
OPTIONS(path "/mnt/formula1dl1986/raw/circuits.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create races table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT, 
  round INT,
  circuitId INT, 
  name STRING,
  date DATE,
  time STRING, 
  url STRING
)
USING CSV
OPTIONS(path "/mnt/formula1dl1986/raw/races.csv", header true);



-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables from JSON files 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create constructor tables 
-- MAGIC ### Single line JSON Simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING JSON
OPTIONS(path "/mnt/formula1dl1986/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create drivers table
-- MAGIC ### Single line JSON with complex structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING)
  USING JSON
  OPTIONS(path "/mnt/formula1dl1986/raw/drivers.json");

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create results table
-- MAGIC ### Single line JSON with simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(
  raceId INT,
      driverId INT,
      constructorId INT,
      number INT,     
      grid INT,
      position INT,     
      positionText STRING,      
      positionOrder INT,
      points FLOAT,     
      laps INT,
      time STRING,     
      milliseconds INT,     
      fastestLap INT,     
      rank INT,
      fastestLapTime STRING,
      fastestLapSpeed DOUBLE,    
      statusId INT
)
USING JSON
OPTIONS(path "/mnt/formula1dl1986/raw/results.json");

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create pit_stop table
-- MAGIC ### Simple JSON with multi line

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pit_stop(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING JSON
OPTIONS (path "/mnt/formula1dl1986/raw/pit_stops.json", multiLine true);

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stop

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create table from list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Lap Times table
-- MAGIC #### multiple csv files

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/formula1dl1986/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating Qualifying Table
-- MAGIC ### JSON file
-- MAGIC ### Multi line
-- MAGIC ### multiple file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING json
OPTIONS(path "/mnt/formula1dl1986/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying
