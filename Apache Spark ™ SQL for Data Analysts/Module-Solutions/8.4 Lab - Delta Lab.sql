-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lab 4 - Delta Lab
-- MAGIC ## Module 8 Assignment
-- MAGIC In this lab, you will continue your work on behalf of Moovio, the fitness tracker company. You will be working with a new set of files that you must move into a "gold-level" table. You will need to modify and repair records, create new columns, and merge late-arriving data. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 1: Create a table
-- MAGIC 
-- MAGIC **Summary:** Create a table from `json` files. 
-- MAGIC 
-- MAGIC Use this path to access the data: <br>
-- MAGIC `"dbfs:/mnt/training/healthcare/tracker/raw.json/"`
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Create a table named `health_tracker_data_2020`
-- MAGIC * Use optional fields to indicate the path you're reading from and epress that the schema should be inferred. 

-- COMMAND ----------

-- ANSWER
DROP TABLE IF EXISTS health_tracker_data_2020;              

CREATE TABLE health_tracker_data_2020                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw.json",
  inferSchema "true"
  );


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 2: Preview the data
-- MAGIC 
-- MAGIC **Summary:**  View a sample of the data in the table. 
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Query the table with `SELECT *` to see all columns
-- MAGIC * Sample 5 rows from the table

-- COMMAND ----------

-- ANSWER
SELECT * FROM health_tracker_data_2020 TABLESAMPLE (5 ROWS)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 3: Count Records
-- MAGIC **Summary:** Write a query to find the total number of records
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Count the number of records in the table
-- MAGIC 
-- MAGIC **Answer the corresponding question in Coursera**

-- COMMAND ----------

-- ANSWER
SELECT COUNT(*) FROM health_tracker_data_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 4: Create a Silver Delta table
-- MAGIC **Summary:** Create a Delta table that transforms and restructures your table
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Drop the existing `month` column
-- MAGIC * Isolate each property of the object in the `value` column to its own column
-- MAGIC * Cast time as timestamp **and** as a date
-- MAGIC * Partition by `device_id`
-- MAGIC * Use Delta to write the table

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE health_tracker_silver 
USING DELTA
PARTITIONED BY (p_device_id)
LOCATION "dbfs:/health_tracker/silver" AS (
SELECT
  value.name,
  value.heartrate,
  CAST(FROM_UNIXTIME(value.time) AS timestamp) AS time,
  CAST(FROM_UNIXTIME(value.time) AS DATE) AS dte,
  value.device_id p_device_id
FROM
  health_tracker_data_2020
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 5: Register table to the metastore
-- MAGIC **Summary:** Register your Silver table to the Metastore
-- MAGIC Steps to complete: 
-- MAGIC * Be sure you can run the cell more than once without throwing an error
-- MAGIC * Write to the location: `/health_tracker/silver`

-- COMMAND ----------

-- ANSWER
DROP TABLE IF EXISTS health_tracker_silver;
CREATE TABLE health_tracker_silver
USING DELTA
LOCATION "/health_tracker/silver"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 6: Check the number of records
-- MAGIC **Summary:** Check to see if all devices are reporting the same number of records
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Write a query that counts the number of records for each device
-- MAGIC * Include your partitioned device id column and the count of those records
-- MAGIC 
-- MAGIC **Answer the corresponding question in Coursera**

-- COMMAND ----------

--ANSWER

SELECT p_device_id, COUNT(*) FROM health_tracker_silver GROUP BY p_device_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 7: Plot records
-- MAGIC **Summary:** Attempt to visually assess which dates may be missing records
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Write a query that will return records from one devices that is **not** missing records as well as the device that seems to be missing records
-- MAGIC * Plot the results to visually inspect the data
-- MAGIC * Identify dates that are missing records
-- MAGIC 
-- MAGIC **Answer the corresponding question in Coursera**

-- COMMAND ----------

--ANSWER
SELECT * FROM health_tracker_silver WHERE p_device_id IN (3,4)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 8: Check for Broken Readings
-- MAGIC **Summary:** Check to see if your data contains records that would indicate a device has misreported data
-- MAGIC Steps to complete: 
-- MAGIC * Create a view that contains all records reporting a negative heartrate
-- MAGIC * Plot/view that data to see which days include broken readings

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW broken_readings
AS (
  SELECT COUNT(*) as broken_readings_count, dte FROM health_tracker_silver
  WHERE heartrate < 0
  GROUP BY dte
  ORDER BY dte
);

SELECT * FROM broken_readings;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 9: Repair records
-- MAGIC **Summary:** Create a view that contains interpolated values for broken readings
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Create a temporary view that will hold all the records you want to update. 
-- MAGIC * Transform the data such that all broken readings (where heartrate is reported as less than zero) are interpolated as the mean of the the data points immediately surrounding the broken reading. 
-- MAGIC * After you write the view, count the number of records in it. 
-- MAGIC 
-- MAGIC **Answer the corresponding question in Coursera** 

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW updates 
AS (
  SELECT name, (prev_amt+next_amt)/2 AS heartrate, time, dte, p_device_id
  FROM (
    SELECT *, 
    LAG(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS prev_amt, 
    LEAD(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS next_amt 
    FROM health_tracker_silver
  ) 
  WHERE heartrate < 0
);

SELECT COUNT(*) FROM updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 10: Read late-arriving data
-- MAGIC **Summary:** Read in new late-arriving data
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Create a new table that contains the late arriving data at this path: `"dbfs:/mnt/training/healthcare/tracker/raw-late.json"`
-- MAGIC * Count the records <br/>
-- MAGIC 
-- MAGIC **Answer the corresponding question in Coursera**

-- COMMAND ----------

--ANSWER
DROP TABLE IF EXISTS late_arriving_data;              

CREATE TABLE late_arriving_data                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw-late.json",
  inferSchema "true"
  );
  

SELECT COUNT(*) FROM late_arriving_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 11: Prepare inserts
-- MAGIC **Summary:** Prepare your new, late-arriving data for insertion into the Silver table
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Create a temporary view that holds the new late-arriving data
-- MAGIC * Apply transformations to the data so that the schema matches our existing Silver table

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW inserts AS (
  SELECT
    value.name,
    value.heartrate,
    CAST(FROM_UNIXTIME(value.time) AS timestamp) AS time,
    CAST(FROM_UNIXTIME(value.time) AS DATE) AS dte,
    value.device_id p_device_id
  FROM
    late_arriving_data
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 12: Prepare upserts
-- MAGIC **Summary:** Prepare a view to upsert to our Silver table
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Create a temporary view that is the `UNION` of the views that hold data you want to insert and data you want to update
-- MAGIC * Count the records
-- MAGIC 
-- MAGIC **Answer the corresponding question in Coursera**

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
    SELECT * FROM updates 
    UNION ALL 
    SELECT * FROM inserts
    );

SELECT COUNT(*) FROM upserts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 13: Perform upserts
-- MAGIC 
-- MAGIC **Summary:** Merge the upserts into your Silver table
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Merge data on the time and device id columns from your Silver table and your upserts table
-- MAGIC * Use `MATCH`conditions to decide whether to apply an update or an insert

-- COMMAND ----------

--ANSWER

MERGE INTO health_tracker_silver                             
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id   
   
WHEN MATCHED THEN                                            
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                        
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 14: Write to gold
-- MAGIC **Summary:** Create a Gold level table that holds aggregated data
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Create a Gold-level Delta table
-- MAGIC * Aggregate heartrate to display the average and standard deviation for each device. 
-- MAGIC * Count the number of records

-- COMMAND ----------

--ANSWER
DROP TABLE IF EXISTS health_tracker_gold;              

CREATE TABLE health_tracker_gold                        
USING DELTA
LOCATION "/health_tracker/gold"
AS 
SELECT 
  AVG(heartrate) AS meanHeartrate,
  STD(heartrate) AS stdHeartrate,
  MAX(heartrate) AS maxHeartrate
FROM health_tracker_silver
GROUP BY p_device_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleanup
-- MAGIC Run the following cell to clean up your workspace. 

-- COMMAND ----------

-- %run .Includes/Classroom-Cleanup


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
