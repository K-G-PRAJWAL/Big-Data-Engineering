-- Databricks notebook source
-- MAGIC 
-- MAGIC %run "./Classroom-Setup"

-- COMMAND ----------

DROP TABLE IF EXISTS DCDataRaw;
CREATE TABLE DCDataRaw
USING parquet                           
OPTIONS (
    PATH "/mnt/training/iot-devices/data-centers/2019-q2-q3"
    );
    
CREATE TABLE IF NOT EXISTS DeviceData     
USING parquet                               
WITH ExplodeSource                        
AS
  (
  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM DCDataRaw
  )
SELECT 
  dc_id,
  key device_type,                         
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level co2Level
  
FROM ExplodeSource;

