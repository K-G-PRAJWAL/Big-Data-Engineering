-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Managing Nested Data with Spark SQL
-- MAGIC 
-- MAGIC In this notebook, you'll be digging into mock data from a group of data centers. A **data center** is a dedicated space where computing and networking equipment is set up to collect, store, process, and distribute data. The continuous operation of centers like this can be crucial to maintaining continuity in business, so environmental conditions must be closely monitored. 
-- MAGIC 
-- MAGIC This example uses mock data from 4 different data centers, each with four different kinds of sensors that periodically collect temperature and CO<sub>2</sub> level readings. Temperature and CO<sub>2</sub> levels are stored as arrays where temperature is collected 12 times per day and CO<sub>2</sub> level is collected 6 times per day. 
-- MAGIC 
-- MAGIC Run the following queries to learn about how to work with and manage nested data in Spark SQL.<br>
-- MAGIC In this notebook, you will: 
-- MAGIC * Work with hierarchical data
-- MAGIC * Use common table expressions (CTE)
-- MAGIC * Create new tables based on CTEs
-- MAGIC * Use `EXPLODE` to manage nested objects

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Getting started
-- MAGIC 
-- MAGIC Run the cell below to set up your classroom environment. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create table 
-- MAGIC 
-- MAGIC The [Databricks File System (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. In practice, this will allow you to access data that has been mounted to your workspace and interact with that storage using directories and file names instead of storage urls. In this lesson, we'll use data from datasets in object storage that has been mounted to the DBFS. We will create a table and explore some of the optional arguments available to us.
-- MAGIC 
-- MAGIC The cell below begins with a `DROP TABLE IF EXISTS`; command. This means that if a table by the given name exists, it will be dropped. If it does not exist, this command does nothing. This will keep our notebook **idempotent**, meaning it could be run more than once without throwing errors or introducing extra files.

-- COMMAND ----------

DROP TABLE IF EXISTS DCDataRaw;
CREATE TABLE DCDataRaw
USING parquet                           
OPTIONS (
    PATH "/mnt/training/iot-devices/data-centers/2019-q2-q3"
    )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### View metadata and "Detailed Table Information"
-- MAGIC 
-- MAGIC In a previous lesson, we used the `DESCRIBE` command to view metadata. Run the command below to see the output when we attach the optional keyword `EXTENDED`. 
-- MAGIC 
-- MAGIC You can find the same information about the schema at the top. Notice that one of our columns contains a `MapType` column, and, within that, a `StructType` field. When working with structured data, like parquet files, and semi-structured data, like JSON files, you will frequently encounter complex data types, like `MapType`, `StructType`, and `ArrayType`. 
-- MAGIC 
-- MAGIC In this example, the `MapType` column holds a JSON object that has a `string` as its **key** and a `struct` field as the **value**. As you work through this notebook, we will unnest and explore that data. Learn more about the data types you will be working with in Spark SQL in the [associated docs](https://spark.apache.org/docs/latest/sql-ref-datatypes.html).
-- MAGIC 
-- MAGIC **Detailed Table Information** contains information about the table's database name, original source file type and location, and more. 

-- COMMAND ----------

DESCRIBE EXTENDED DCDataRaw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### View a sample
-- MAGIC It may you help understand the data if we view a few rows. Instead of simply returning the top rows, we can get a random sampling of rows using the function `RAND()` to return random rows and the `LIMIT` keyword to set the number of rows we want to see. 

-- COMMAND ----------

SELECT * FROM DCDataRaw
ORDER BY RAND()
LIMIT 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explode a nested object
-- MAGIC We can observe from the output that the `source` column contains a nested object with named `key-value` pairs. We'll use `EXPLODE` to get a closer look at the data in that column. 
-- MAGIC 
-- MAGIC **`EXPLODE`** is used with arrays and elements of a map expression. When used with an array, it splits the elements into multiple rows. Used with a map, as in this example, it splits the elements of a map into multiple rows and columns and uses the default names, `key` and `value`, to name the new columns. This data structure is mapped such that each `key`, the name of a certain device, holds an object, `value`, containing information about that device.

-- COMMAND ----------

SELECT EXPLODE (source)
FROM DCDataRaw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Common Table Expressions
-- MAGIC 
-- MAGIC Common Table Expressions (CTE) are supported in Spark SQL. A CTE provides a temporary result set which you can then use in a `SELECT` statement. These are different from temporary views in that they cannot be used beyond the scope of a single query. In this case, we will use the CTE to get a closer look at the nested data without writing a new table or view. CTEs use the `WITH` clause to start defining the expression.
-- MAGIC 
-- MAGIC Notice that after we explode the source column, we can access individual properties in the `value` field by using dot notation with the property name. 

-- COMMAND ----------

WITH ExplodeSource  -- specify the name of the result set we will query
AS                  
(                   -- wrap a SELECT statement in parentheses
  SELECT            -- this is the temporary result set you will query
    dc_id,
    to_date(date) AS date,
    EXPLODE (source)
  FROM
    DCDataRaw
)
SELECT             -- write a select statment to query the result set
  key,
  dc_id,
  date,
  value.description,  
  value.ip,
  value.temps,
  value.co2_level
FROM               -- this query is coming from the CTE we named
  ExplodeSource;  
                  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Table as Select (CTAS)
-- MAGIC 
-- MAGIC CTEs like those in the cell above are temporary and cannot be queried again. In the next cell, we demonstrate how you create a table using the common table expression syntax. 
-- MAGIC 
-- MAGIC In Spark SQL, you can populate a new table with input data from a `SELECT` statement. The following is an example where we create a new table, `DeviceData`, using the CTE syntax we used in the previous cell. In this example, we rename the `key` column to `device_type`. 

-- COMMAND ----------

DROP TABLE IF EXISTS DeviceData;
CREATE TABLE DeviceData                 
USING parquet
WITH ExplodeSource                       -- The start of the CTE from the last cell
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
  value.co2_level
  
FROM ExplodeSource;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run a `SELECT` all to view the new table.

-- COMMAND ----------

SELECT * FROM DeviceData

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
