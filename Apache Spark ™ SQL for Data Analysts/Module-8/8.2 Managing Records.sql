-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Managing Records
-- MAGIC 
-- MAGIC In the previous reading we demonstrated how to create a Delta table. We used basic data exploration strategies to identify two problems within the data. In this notebook, we will demonstrate how to correct those problems and write to a new, clean gold-level table that you can use for queries. Also, we will demonstrate how to repair and correct records
-- MAGIC 
-- MAGIC In this notebook, you will:
-- MAGIC * Use a window function to interpolate missing values
-- MAGIC * Update a Delta table
-- MAGIC * Check the version history in a Delta table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Getting started
-- MAGIC 
-- MAGIC Run the cell below to set up your classroom environment

-- COMMAND ----------

-- MAGIC %run ../Includes/8-2-Setup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Repairing Records
-- MAGIC 
-- MAGIC In the previous reading, we found two problems in the data: 
-- MAGIC 
-- MAGIC 1. We were missing records on a single device for a period of three days
-- MAGIC 1. There was at least one broken reading (heartrate less than zero) per day in our set
-- MAGIC 
-- MAGIC We will start by demonstrating how to merge a set of updates and insertions to repair these problems. 
-- MAGIC 
-- MAGIC First, we will work on the broken sensor readings. Previously, you used a window function, paired with the `AVG` function, to calculate an average value over a group of rows. Here, we will use a window function, paired with the built-in functions `LAG` and `LEAD`, to interpolate values to replace the broken readings. 
-- MAGIC 
-- MAGIC **`LAG`**: fetches data from the previous row. [Learn more](https://spark.apache.org/docs/latest/api/sql/#lag). <br>
-- MAGIC **`LEAD`**: fetches data from a subsequent row. [Learn more](https://spark.apache.org/docs/latest/api/sql/#lead).<br>
-- MAGIC 
-- MAGIC Examine the code in the next cell. 
-- MAGIC 
-- MAGIC `line 1`: We create or replace a temporary view names `updates`<br>
-- MAGIC `line 2`: We are using a CTAS pattern to create this new view<br>
-- MAGIC `line 3`: We select a subgroup of columns to include from the window function defined in lines 5 - 8. Note the expression `(prev_amt+next_amt)/2`. For any missing entry, we calculate a new data point that is the mean of the previous entry and the subsequent entry. These values are defined in the window function below <br>
-- MAGIC `line 4`: Indicates the window function as the source. The parenthesis marks the start of the window function<br>
-- MAGIC `line 5`: Select all columns from `health_tracker_silver`<br>
-- MAGIC `line 6`: `LAG` gets the `heartrate` from the previous row. We define the window by `device_id` and `dte` so that the calculation is applied to each missing value **by device** <br>
-- MAGIC `line 9`: Marks the end of the window function<br>
-- MAGIC `line 10`: Identifies that this calulation should apply **only** where the heartrate reading is less than 0
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> **Interpolation** is a type of estimation where we construct new data points based on a set of known data points. 

-- COMMAND ----------

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
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Check schema
-- MAGIC 
-- MAGIC We will want to use the values in `updates` to update our `health_tracker_silver` table. Let's check both schemas to see if they match. 

-- COMMAND ----------

DESCRIBE updates

-- COMMAND ----------

DESCRIBE health_tracker_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Late-arriving data
-- MAGIC 
-- MAGIC We're ready to update our silver table with our interpolated values, but before we do, we find out that those missing readings have finally come through! We can get that data ready to merge with our other updates. 
-- MAGIC 
-- MAGIC Run the cell below to read in the raw data. 

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_data_2020_02_late;              

CREATE TABLE health_tracker_data_2020_02_late                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw-late.json",
  inferSchema "true"
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prepare inserts
-- MAGIC 
-- MAGIC We can apply the same transformations we used to create our `health_tracker_silver` table on this raw data. This we'll give us a view with the same schema as our other tables and views.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW inserts AS (
  SELECT
    value.name,
    value.heartrate,
    CAST(FROM_UNIXTIME(value.time) AS timestamp) AS time,
    CAST(FROM_UNIXTIME(value.time) AS DATE) AS dte,
    value.device_id p_device_id
  FROM
    health_tracker_data_2020_02_late
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prepare upserts
-- MAGIC 
-- MAGIC The word "upsert" is a portmanteau of the words "update" and "insert," and this is what it does. An upsert will update records where some criteria are met and otherwise will insert the record. We create a view that is the union of our `updates` and `inserts` and holds all records we would like to add and modify.
-- MAGIC 
-- MAGIC Here, we use `UNION ALL` to capture all records in both views, even duplicates. 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
    SELECT * FROM updates 
    UNION ALL 
    SELECT * FROM inserts
    )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Perform upsert 
-- MAGIC 
-- MAGIC When upserting into an existing Delta table, use Spark SQL to perform the merge from another registered table or view. The Transaction Log records the transaction, and the Metastore immediately reflects the changes.  
-- MAGIC 
-- MAGIC The merge appends both the new/inserted files and the files containing the updates to the Delta file directory. The transaction log tells the Delta reader which file to use for each record.
-- MAGIC 
-- MAGIC This operation is similar to the SQL `MERGE` command but has added support for deletes and other conditions in updates, inserts, and deletes. In other words, using the Spark SQL command `MERGE` provides full support for an upsert operation.
-- MAGIC 
-- MAGIC Use the comments to better understand how this command integrates records from our existing tables and views. 
-- MAGIC 
-- MAGIC Read more about `MERGE INTO` [here](https://docs.databricks.com/spark/latest/spark-sql/language-manual/merge-into.html#merge-into--delta-lake-on-databricks).

-- COMMAND ----------

MERGE INTO health_tracker_silver                            -- the MERGE instruction is used to perform the upsert
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time travel
-- MAGIC 
-- MAGIC Let's check the number of records in the different versions of our tables. 
-- MAGIC 
-- MAGIC Version 1 shows the data after we added records from February. Recall that this is where we first discovered the missing records. 
-- MAGIC 
-- MAGIC The current version shows everything including the records we upcerted.

-- COMMAND ----------

-- VERSION 1
SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 1

-- COMMAND ----------

-- CURRENT VERSION
SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Describe history
-- MAGIC  You can check the full history of a Delta table including the operation, user, and so on for each new write to a table. 
-- MAGIC  

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Write to gold
-- MAGIC 
-- MAGIC So far, we have ingested raw (bronze-level) data and applied transformations to create a silver table. We have used Spark SQL to explore and transform that data further, adding new values when we found collection errors and updating the table to reflect late-arriving data. Now that our data is clean and polished, we can write to a gold table. Gold tables are used to hold business level aggregates. When we create this table, we also apply aggregate functions to several columns. 

-- COMMAND ----------

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
GROUP BY p_device_id


-- COMMAND ----------

SELECT
  *
FROM
  health_tracker_gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleanup
-- MAGIC Run the next cell to clean up your classroom environment

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Great work! Now that you've got a basic understanding of how data moves through the Delta architecture, we're ready to get back to analytics. In the next reading, we'll see how to write high-performance Spark queries with Databricks Delta. 

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
