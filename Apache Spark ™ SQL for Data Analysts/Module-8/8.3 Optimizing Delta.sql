-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Optimizing Delta
-- MAGIC 
-- MAGIC In this notebook, you'll see some examples of how you can optimize your queries using Delta Engine, which is built-in to the Databricks Runtime 7.0. It is also part of open source [Delta Lake](https://delta.io/).
-- MAGIC 
-- MAGIC The data contains information about US-based flight schedules from 2008. It is made available to us via [Databricks Datasets](https://docs.databricks.com/data/databricks-datasets.html). 
-- MAGIC 
-- MAGIC First, we will create a standard table using Parquet format and then we'll run a query to observe the timing. 
-- MAGIC 
-- MAGIC Then, we'll run the same query on a Delta table using Delta Engine optimizations and compare the two. 
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Databricks includes a variety of datasets that you can use to continue learning, or just for practice! Check out the docs for copyable Python code that you can use to see what sets are available. 
-- MAGIC 
-- MAGIC Run the cell below to set up your classroom environment. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Parquet table
-- MAGIC Run the command below to create a Parquet table. 

-- COMMAND ----------

DROP TABLE IF EXISTS flights;
-- Create a standard table and import US based flights for year 2008
-- USING Clause: Specify parquet format for a standard table
-- PARTITIONED BY clause: Orginize data based on "Origin" column (Originating Airport code).
-- FROM Clause: Import data from a csv file. 
CREATE TABLE flights
USING
  parquet
PARTITIONED BY
  (Origin)
SELECT
  _c0 AS Year,
  _c1 AS MONTH,
  _c2 AS DayofMonth,
  _c3 AS DayOfWeek,
  _c4 AS DepartureTime,
  _c5 AS CRSDepartureTime,
  _c6 AS ArrivalTime,
  _c7 AS CRSArrivalTime,
  _c8 AS UniqueCarrier,
  _c9 AS FlightNumber,
  _c10 AS TailNumber,
  _c11 AS ActualElapsedTime,
  _c12 AS CRSElapsedTime,
  _c13 AS AirTime,
  _c14 AS ArrivalDelay,
  _c15 AS DepartureDelay,
  _c16 AS Origin,
  _c17 AS Destination,
  _c18 AS Distance,
  _c19 AS TaxiIn,
  _c20 AS TaxiOut,
  _c21 AS Cancelled,
  _c22 AS CancellationCode,
  _c23 AS Diverted,
  _c24 AS CarrierDelay,
  _c25 AS WeatherDelay,
  _c26 AS NASDelay,
  _c27 AS SecurityDelay,
  _c28 AS LateAircraftDelay
FROM                              -- This table is being read in directly from a csv file. 
  csv.`dbfs:/databricks-datasets/asa/airlines/2008.csv` 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Highest monthly total (Parquet)
-- MAGIC 
-- MAGIC Run the query to get the top 20 cities with the highest monthly total flights on the first day of the week. Be sure to note the time when the query finishes. 

-- COMMAND ----------

SELECT Month, Origin, count(*) as TotalFlights 
FROM flights
WHERE DayOfWeek = 1 
GROUP BY Month, Origin 
ORDER BY TotalFlights DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Delta Table
-- MAGIC Run the query below to compare Delta to Parquet. Note, this is the exact same command running on the exact same cluster configuration. Recall that the two operations take roughly the same amount of "work" from Spark. We have to read in a huge csv file, partition it by origin, and store it in a new, columnar format. Plus, Delta is creating a transaction log and tagging the files with important and useful metadata! 

-- COMMAND ----------

DROP TABLE IF EXISTS flights;
-- Create a standard table and import US based flights for year 2008
-- USING Clause: Specify "delta" format instead of the standard parquet format
-- PARTITIONED BY clause: Orginize data based on "Origin" column (Originating Airport code).
-- FROM Clause: Import data from a csv file.
CREATE TABLE flights
USING
  delta
PARTITIONED BY
  (Origin)
SELECT
  _c0 AS Year,
  _c1 AS MONTH,
  _c2 AS DayofMonth,
  _c3 AS DayOfWeek,
  _c4 AS DepartureTime,
  _c5 AS CRSDepartureTime,
  _c6 AS ArrivalTime,
  _c7 AS CRSArrivalTime,
  _c8 AS UniqueCarrier,
  _c9 AS FlightNumber,
  _c10 AS TailNumber,
  _c11 AS ActualElapsedTime,
  _c12 AS CRSElapsedTime,
  _c13 AS AirTime,
  _c14 AS ArrivalDelay,
  _c15 AS DepartureDelay,
  _c16 AS Origin,
  _c17 AS Destination,
  _c18 AS Distance,
  _c19 AS TaxiIn,
  _c20 AS TaxiOut,
  _c21 AS Cancelled,
  _c22 AS CancellationCode,
  _c23 AS Diverted,
  _c24 AS CarrierDelay,
  _c25 AS WeatherDelay,
  _c26 AS NASDelay,
  _c27 AS SecurityDelay,
  _c28 AS LateAircraftDelay
FROM
  csv.`dbfs:/databricks-datasets/asa/airlines/2008.csv`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimize your table
-- MAGIC 
-- MAGIC If your organization continuously writes data to a Delta table, it will over time accumulate a large number of files, especially if you add data in small batches. For analysts, a common complaint in querying data lakes is read efficiency; and having a large collection of small files to sift through everytime data is queried can create performance problems. Ideally, a large number of small files should be rewritten into a smaller number of larger files on a regular basis, which will improve the speed of read queries from a table. This is known as compaction. You can compact a table using the `OPTIMIZE` command shown below. 
-- MAGIC 
-- MAGIC Z-ordering co-locates column information (recall that Delta is columnar storage). Co-locality is used by Delta Lake data-skipping algorithms to dramatically reduce the amount of data that needs to be read. You can specify multiple columns for ZORDER BY as a comma-separated list. However, the effectiveness of the locality drops with each additional column. Read more about optimizing Delta tables [here](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html).

-- COMMAND ----------

OPTIMIZE flights ZORDER BY (DayofWeek);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Rerun the query
-- MAGIC Run the query below to compare performance for a standard Parquet table with an optimized Delta table. 

-- COMMAND ----------

SELECT Month, Origin, count(*) as TotalFlights 
FROM flights
WHERE DayOfWeek = 1 
GROUP BY Month, Origin 
ORDER BY TotalFlights DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Cache 
-- MAGIC 
-- MAGIC Using the Delta cache is an excellent way to optimize performance. Note: The Delta cache is *not* the same as caching in Apache Spark, which we talked about in Module 4. One notable difference is that the Delta cache is stored entirely on the local disk, so that memory is not taken away from other operations within Spark. When enabled, the Delta cache automatically creates a copy of a remote file in local storage so that successive reads are significantly sped up. Unfortunately, to enable it, you must choose a cluster type that is not available in Databricks Community Edition. 
-- MAGIC 
-- MAGIC To better understand the differences between Delta caching and Apache Spark caching, please read, ["Delta and Apache Spark caching."](https://docs.databricks.com/delta/optimizations/delta-cache.html#delta-and-apache-spark-caching)

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
