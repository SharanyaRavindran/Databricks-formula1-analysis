-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style = "coclor:Blue; text-align:center; font-family:Ariel">Report on Dominant Fromula1 Teams</h2>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT constructor_name AS team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY avg(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
GROUP BY constructor_name
HAVING count(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT team_name FROM v_dominant_teams WHERE team_rank <=5

-- COMMAND ----------

SELECT race_year,
       constructor_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points      
FROM f1_presentation.calculated_race_results
WHERE constructor_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <=5)
GROUP BY race_year, constructor_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT race_year,
       constructor_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points      
FROM f1_presentation.calculated_race_results
WHERE constructor_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <=5)
GROUP BY race_year, constructor_name
ORDER BY race_year, avg_points DESC
