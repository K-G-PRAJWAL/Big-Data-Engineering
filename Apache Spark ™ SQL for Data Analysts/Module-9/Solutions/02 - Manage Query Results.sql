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

-- MAGIC %run ../Utilities/02-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Question 1: Dedupe Sort
-- MAGIC ### Summary
-- MAGIC Deduplicate and sort data on products that need to be restocked in a grocery store.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query on the **`products`** table that achieves the following:
-- MAGIC * Removes duplicate rows 
-- MAGIC * Sorts rows by the **`aisle`** column in ascending order (with nulls appearing last), and then by **`price`** in ascending order
-- MAGIC * Stores the results into a temporary view named  **`q1Results`**
-- MAGIC    
-- MAGIC    
-- MAGIC A properly completed solution should return both a DataFrame that looks similar to this:
-- MAGIC 
-- MAGIC |itemId|amount|aisle|price|
-- MAGIC |---|---|---|---|
-- MAGIC |  9958|    64|    2|    2|
-- MAGIC |  1432|    23|    3|   24|
-- MAGIC |  3242|    14|    5|    5|
-- MAGIC |...|...|...|...|
-- MAGIC |  7064|    34| null|   24|
-- MAGIC |  0244|    7| null|   36|

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW q1Results AS
  SELECT DISTINCT * FROM products 
    ORDER BY aisle ASC NULLS LAST, price ASC;
    
SELECT * FROM q1Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 2: Limit Results 
-- MAGIC ### Summary
-- MAGIC Return the top five results for data that matches a set of criteria.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query on the table **`raceResults`** that achieves the following: 
-- MAGIC * Casts **`lastFinish`** column as date and renames to **`raceDate`**
-- MAGIC * Sorts rows by the **`winOdds`** column in descending order
-- MAGIC * Limits the results to the top 5 **`winOdds`**
-- MAGIC * Stores the results into a temporary view named  **`q2Results`**
-- MAGIC    
-- MAGIC A properly completed solution should return a DataFrame that looks similar to this:
-- MAGIC 
-- MAGIC |name|winOdds|raceDate|
-- MAGIC |--- |-------|----------|
-- MAGIC | Dolor Incididunt|    .9634252|    2015-07-29| 
-- MAGIC | Excepteur Mollit|    .9524401|    2019-08-15| 
-- MAGIC | Magna Ad        |    .9420442|    2017-05-12| 
-- MAGIC |  Sed In|.9325211| 2014-08-29|
-- MAGIC |  Qui Cupidat| .9242451| 2011-08-23| 

-- COMMAND ----------

--ANSWER

CREATE OR REPLACE TEMPORARY VIEW q2Results AS
  SELECT name, 
      winOdds, 
      to_date(lastFinish) AS raceDate
  FROM raceResults
  WHERE winOdds > 0.9 
  ORDER BY winOdds DESC
  LIMIT 5;

SELECT * FROM q2Results;


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
