-- Databricks notebook source
CREATE TABLE f1_presentation.dominanat_team
AS
SELECT constructor_name AS team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY constructor_name
HAVING count(1) >= 100
ORDER BY avg_points DESC
