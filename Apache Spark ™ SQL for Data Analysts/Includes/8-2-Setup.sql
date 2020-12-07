-- Databricks notebook source
-- MAGIC 
-- MAGIC %run "./Classroom-Setup"

-- COMMAND ----------

-- MAGIC %fs rm -r /health_tracker/gold

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_data_2020_01;              

CREATE TABLE health_tracker_data_2020_01                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw.json/health_tracker_data_2020_1.json",
  inferSchema "true"
  );

-- COMMAND ----------

CREATE OR REPLACE TABLE health_tracker_silver 
USING DELTA
PARTITIONED BY (p_device_id)
LOCATION "/health_tracker/silver" AS (
SELECT
  value.name,
  value.heartrate,
  CAST(FROM_UNIXTIME(value.time) AS timestamp) AS time,
  CAST(FROM_UNIXTIME(value.time) AS DATE) AS dte,
  value.device_id p_device_id
FROM
  health_tracker_data_2020_01
)


-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_silver;

CREATE TABLE health_tracker_silver
USING DELTA
LOCATION "/health_tracker/silver"

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_data_2020_02;              

CREATE TABLE health_tracker_data_2020_02                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw.json/health_tracker_data_2020_2.json",
  inferSchema "true"
  );

-- COMMAND ----------

INSERT INTO
  health_tracker_silver
SELECT
  value.name,
  value.heartrate,
  CAST(FROM_UNIXTIME(value.time) AS timestamp) AS time,
  CAST(FROM_UNIXTIME(value.time) AS DATE) AS dte,
  value.device_id p_device_id
FROM
  health_tracker_data_2020_02

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW broken_readings
AS (
  SELECT COUNT(*) as broken_readings_count, dte FROM health_tracker_silver
  WHERE heartrate < 0
  GROUP BY dte
  ORDER BY dte
)

