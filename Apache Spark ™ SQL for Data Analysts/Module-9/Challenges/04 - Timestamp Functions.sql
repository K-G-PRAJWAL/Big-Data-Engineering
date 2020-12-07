-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Tables
-- MAGIC Run the cell below to create tables for the questions in this notebook. 

-- COMMAND ----------

-- MAGIC %run ../Utilities/04-CreateTables

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Question 1: Extract Year and Month
-- MAGIC ### Summary
-- MAGIC Extract the year and month from the **`Timestamp`** field in the table **`timetable1`** and store records with only the 12th month into **`q1Results`** table.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query that achieves the following: 
-- MAGIC   * Extracts the **`year`** and **`month`** from the **`Timestamp`** column from the table **`timetable1`**
-- MAGIC     - **`Timestamp`** is an integer representing seconds since midnight on January 1st, 1970 (e.g. 1519344286). You must cast it as a `timestamp` to to extract years and months. 
-- MAGIC   * Filters records to include only month 12
-- MAGIC   * Stores the resulting records in a temporary view named **`q1Results`** with the following schema.
-- MAGIC 
-- MAGIC | column | type |
-- MAGIC |--------|--------|
-- MAGIC | Date | timestamp |
-- MAGIC | Year | integer |
-- MAGIC | Month | integer |
-- MAGIC 
-- MAGIC 
-- MAGIC A properly completed solution should produce a DataFrame similar to this sample output:
-- MAGIC 
-- MAGIC |               Date|Year|Month|
-- MAGIC |-------------------|----|-----|
-- MAGIC |2010-12-15 21:36:55|2010|   12|
-- MAGIC |2002-12-01 11:17:54|2002|   12|
-- MAGIC |2017-12-13 11:28:03|2017|   12|

-- COMMAND ----------

-- TODO  Answer 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 2: Extract year, month, and day of year
-- MAGIC ### Summary
-- MAGIC Extract the **year**, **month** and **dayofyear**  from the **`Timestamp`** field in the table **`timetable2`** and return records for **only** the 4th month.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query that achieves the following:
-- MAGIC * Create **`Year`**, **`Month`** and **`DayOfYear`** columns from the **`Timestamp`** column in the **`timetable2`** table
-- MAGIC    - **`Timestamp`** is an integer representing seconds since midnight on January 1st, 1970 (e.g. 1519344286). You must cast it as a `timestamp` to to extract years, months, and day of year. 
-- MAGIC * Filter records to include only month 4 
-- MAGIC * Stores the records in a temporary table named  **`q2Results`** with the following schema:
-- MAGIC 
-- MAGIC | column    | type      |
-- MAGIC |-----------|-----------|
-- MAGIC | Date      | timestamp |
-- MAGIC | Year      | integer   |
-- MAGIC | Month     | integer   |
-- MAGIC | DayOfYear | integer   |
-- MAGIC <br>
-- MAGIC 
-- MAGIC * A properly completed solution should produce a DataFrame similar to this sample output:
-- MAGIC 
-- MAGIC |               Date|Year|Month| DayOfYear |
-- MAGIC |-------------------|----|-----|-----------|
-- MAGIC |2002-04-22 06:41:39|2002|    4|      112  |
-- MAGIC |2012-04-01 05:00:06|2012|    4|       92  |
-- MAGIC |2019-04-05 12:38:42|2019|    4|       95  |

-- COMMAND ----------

-- TODO  Answer 2


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
