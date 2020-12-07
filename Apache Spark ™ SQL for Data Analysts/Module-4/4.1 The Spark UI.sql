-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

DROP TABLE IF EXISTS People10M;
CREATE TABLE People10M
USING csv
OPTIONS (
path "/mnt/training/dataframes/people-10m.csv",
header "true");

DROP TABLE IF EXISTS ssaNames;
CREATE TABLE ssaNames USING parquet OPTIONS (
  path "/mnt/training/ssn/names.parquet",
  header "true"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Catalog Error

-- COMMAND ----------

SELECT
  firstName,
  lastName,
  birthDate
FROM
  People10M
WHERE
  year(birthDate) > 1990
  AND gender = 'F'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Plan Optimization Example

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW joined AS
SELECT People10m.firstName,
  to_date(birthDate) AS date
FROM People10m
  JOIN ssaNames ON People10m.firstName = ssaNames.firstName;

CREATE OR REPLACE TEMPORARY VIEW filtered AS
SELECT firstName,count(firstName)
FROM joined
WHERE
  date >= "1980-01-01"
GROUP BY
  firstName, date;


-- COMMAND ----------

SELECT * FROM  filtered;

-- COMMAND ----------

CACHE TABLE filtered;

-- COMMAND ----------

SELECT * FROM filtered;

-- COMMAND ----------

SELECT * FROM filtered WHERE firstName = "Latisha";

-- COMMAND ----------

UNCACHE TABLE IF EXISTS filtered;

-- COMMAND ----------

SELECT * FROM filtered WHERE firstName = "Latisha";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Partitions

-- COMMAND ----------

DROP TABLE IF EXISTS bikeShare;
CREATE TABLE bikeShare
USING csv
OPTIONS (
  path "/mnt/training/bikeSharing/data-001/hour.csv",
  header "true")

-- COMMAND ----------

SELECT
  *
FROM
  bikeShare
WHERE
  hr = 10

-- COMMAND ----------

DROP TABLE IF EXISTS bikeShare_partitioned;
CREATE TABLE bikeShare_partitioned
PARTITIONED BY (p_hr)
  AS
SELECT
  instant,
  dteday,
  season, 
  yr,
  mnth,
  hr as p_hr,
  holiday,
  weekday, 
  workingday,
  weathersit,
  temp
FROM
  bikeShare

-- COMMAND ----------

SELECT * FROM bikeShare_partitioned WHERE p_hr = 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Beware of small files! 

-- COMMAND ----------

DROP TABLE IF EXISTS bikeShare_instant;
CREATE TABLE bikeShare_instant
PARTITIONED BY (p_instant)
  AS
SELECT
  instant AS p_instant,
  dteday,
  season, 
  yr,
  mnth,
  hr
  holiday,
  weekday, 
  workingday,
  weathersit,
  temp
FROM
  bikeShare;

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Citations
-- MAGIC Bike Sharing Data<br>
-- MAGIC 
-- MAGIC [1] Fanaee-T, Hadi, and Gama, Joao, Event labeling combining ensemble detectors and background knowledge, Progress in Artificial Intelligence (2013): pp. 1-15, Springer Berlin Heidelberg, doi:10.1007/s13748-013-0040-3.
-- MAGIC 
-- MAGIC @article{ year={2013}, issn={2192-6352}, journal={Progress in Artificial Intelligence}, doi={10.1007/s13748-013-0040-3}, title={Event labeling combining ensemble detectors and background knowledge}, url={http://dx.doi.org/10.1007/s13748-013-0040-3}, publisher={Springer Berlin Heidelberg}, keywords={Event labeling; Event detection; Ensemble learning; Background knowledge}, author={Fanaee-T, Hadi and Gama, Joao}, pages={1-15} }

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
