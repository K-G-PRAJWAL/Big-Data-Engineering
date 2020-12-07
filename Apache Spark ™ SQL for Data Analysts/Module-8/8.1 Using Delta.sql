-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using Delta
-- MAGIC 
-- MAGIC Moovio, the fitness tracker company, is in the process of migrating non-Delta workloads to Delta Lake. You have access to the files that hold current data that you can experiment with while learning about how to work with Delta Lake. Creating Delta tables is as easy as issuing the command, `USING DELTA`. Get started by reading through the following cells and run the corresponding queries to create and modify Delta tables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Getting started
-- MAGIC Run the cell below to set up your classroom environment. 

-- COMMAND ----------

-- MAGIC %run "../Includes/Classroom-Setup"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create table
-- MAGIC 
-- MAGIC To start, we will create a table of the raw data we've been provided. We're using only a small sample of the available data, so this set is limited to 5 devices over the course of one month. The raw files are in the `.json` format. 

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_data_2020_01;              

CREATE TABLE health_tracker_data_2020_01                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw.json/health_tracker_data_2020_1.json",
  inferSchema "true"
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preview data
-- MAGIC 
-- MAGIC Before we do anything else, let's quickly inspect the data by viewing a sample.

-- COMMAND ----------

SELECT * FROM health_tracker_data_2020_01 TABLESAMPLE (5 ROWS)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Create Delta table
-- MAGIC 
-- MAGIC This example, so far, is of a Bronze level table. We can display the raw data, but it is not easily queryable. Our next step is to create a cleaned Silver table. This table may flow into several business aggregate Gold level tables later on. In this step, we'll focus on creating an easily queryable table that includes most or all of the data, with all columns accurately typed and object properties unpacked into individual columns. 
-- MAGIC 
-- MAGIC 
-- MAGIC Recall that a Delta table consists of three things: 
-- MAGIC 
-- MAGIC 1. The Delta files (in object storage)
-- MAGIC 1. The Delta [Transaction Log](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html) saved with the Delta files in object storage. 
-- MAGIC 1. The Delta table registered in the [Metastore](https://docs.databricks.com/data/metastores/index.html#metastores) 
-- MAGIC 
-- MAGIC Run the cell below to create a new table using `DELTA`. This step registers your table in the metastore, converts your files to Delta and creates the transaction log, which will hold the record of every transaction that is performed on this table. 

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

-- MAGIC %md
-- MAGIC Great! You have created your first Delta table! Run the `DESCRIBE DETAIL` command to view table details. You can see that the table format is `delta` and  it is stored in the location you specified.

-- COMMAND ----------

DESCRIBE DETAIL health_tracker_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Read in new data
-- MAGIC 
-- MAGIC Recall that we created that table with just one month of data. Now let's see how we can add new data to that table. 
-- MAGIC 
-- MAGIC Run the cell below to read in the new raw file. 

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_data_2020_02;              

CREATE TABLE health_tracker_data_2020_02                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw.json/health_tracker_data_2020_2.json",
  inferSchema "true"
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Append files
-- MAGIC 
-- MAGIC We can append the next month of of records to the existing table using the `INSERT INTO` command. We will transform the new data to match the existing schema. 

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

-- MAGIC %md
-- MAGIC ## Time Travel: Count records in the previous table
-- MAGIC 
-- MAGIC Let's count the records to verify that the append went as expected. First, we can write a query to show the count before we appended new records. Delta Lake can query an earlier version of a Delta table using a feature known as [time travel](https://docs.databricks.com/delta/quick-start.html#query-an-earlier-version-of-the-table-time-travel). 
-- MAGIC 
-- MAGIC We demonstrate querying the data as of version 0, which is the initial conversion of the table from Parquet. 
-- MAGIC 
-- MAGIC **`5 devices * 24 hours * 31 days`** **`=`** **`3720 records`**

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Count records in our current table
-- MAGIC 
-- MAGIC Now, let's count the records to see if our new data was appended as expected. Note that this data is from February 2020, which had 29 days because 2020 was a leap year. We are still working with 5 devices, with heartrate readings occurring once an hour.
-- MAGIC 
-- MAGIC **`5 devices * 24 hours * 29 days`**   **`+`**   **`3720`** **`=`** **`7200 records`**

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver 

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Find missing records by device<br> 
-- MAGIC 
-- MAGIC Let's see if we can identify which device(s) are missing records. 
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The absence of records from the last few days of the month shows a phenomenon that may often occur in a production data pipeline: **late-arriving data**. This can create problems in some of the other data storage and management models we talked about. If our analytics runs on stale or incomplete data, we  may draw incorrect conclusions or make bad predicitions. Delta Lake allows us to process data as it arrives and is prepared to handle the occurrence of late arriving data.

-- COMMAND ----------

SELECT p_device_id, COUNT(*) FROM health_tracker_silver GROUP BY p_device_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Plot Records
-- MAGIC We can run a query and use visualization tools to find out more about which dates or times are missing. For this query, it may be helpful to compare two devices, even though we're showing only one is missing data. Run the cell below to query the table. Then, click on the chart icon to plot the data. 
-- MAGIC 
-- MAGIC To set up your graph: 
-- MAGIC * Click `Plot Options`
-- MAGIC * Drag `dte` into the Keys dialog 
-- MAGIC * Drag `p_device_id`into the Series Groupings dialog
-- MAGIC * Drag `heartrate` into the values dialog
-- MAGIC * Choose `COUNT` as your Aggregation type (the dropdown in the lower left corner)
-- MAGIC * Select "Bar Chart" as your display type. 

-- COMMAND ----------

SELECT * FROM health_tracker_silver WHERE p_device_id IN (3,4)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## Find Broken Readings 
-- MAGIC 
-- MAGIC It's always useful to check for errant readings. Think about this scenario. Is there any reading would seem impossible? 
-- MAGIC 
-- MAGIC Since this is heartrate date, we should expect that everyone who is using the tracker has a heartbeat. Let's check the data to see if we've got any data that might point to a faulty reading. 
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We  use a temporary view so that we can access this data again later. 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW broken_readings
AS (
  SELECT COUNT(*) as broken_readings_count, dte FROM health_tracker_silver
  WHERE heartrate < 0
  GROUP BY dte
  ORDER BY dte
)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## View broken readings
-- MAGIC 
-- MAGIC Run the cell and then create a a visualization that will help us get a sense of how many broken readings exist and how they are spread across the data. 
-- MAGIC 
-- MAGIC To visualize this view: 
-- MAGIC * Run the cell
-- MAGIC * Click the chart icon
-- MAGIC * Choose 'dte' as the Key and `broken_readings_count` as Values. 
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You should notice that most days have at least one broken reading and that some days have more than one. 

-- COMMAND ----------

SELECT * FROM broken_readings;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean-up
-- MAGIC Run the next cell to clean up your classroom enviroment

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Great job! You're officially working with Delta Lake! In the next reading, you'll continue your work to repair the broken data and missing values we discovered here. 

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
