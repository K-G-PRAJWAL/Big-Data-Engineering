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

-- MAGIC %run ../Utilities/07-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 1: Transform
-- MAGIC ### Summary
-- MAGIC Use the **`TRANSFORM`** function and the table **`finances`** to calculate **`interest`** for all cards issued to each user. 
-- MAGIC 
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query that achieves the following: 
-- MAGIC * Displays cardholder's **`firstName`**, **`lastName`**, and a new column named **`interest`**
-- MAGIC * Uses **`TRANSFORM`** to extract charges for each card in the expenses column and calculates interest owed assuming a rate of 6.25%. 
-- MAGIC * Stores the new values as an array in the interest column
-- MAGIC * Stores results in a temporary table named `q1Results`
-- MAGIC 
-- MAGIC A properly completed solution should return a view that looks similar to this:
-- MAGIC 
-- MAGIC | firstName | lastName | interest |
-- MAGIC |----------- |---------|----------|
-- MAGIC |Lance|Da Costa|[138.9, 373.55, 158.97]|

-- COMMAND ----------

--ANSWER
SELECT firstName, lastName, 
TRANSFORM(expenses, card -> ROUND(card.charges * 0.0625, 2)) AS interest 
FROM finances


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 2: Exists
-- MAGIC ### Summary
-- MAGIC Use the table from Question 1, **`finances`**, to flag users whose records indicate that they made a late payment. 
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query that achieves the following: 
-- MAGIC * Displays cardholder's **`firstName`**, **`lastName`**, and a new column named **`lateFee`**
-- MAGIC * Uses the EXISTS function to flag customers who have made been charged a late payment fee.
-- MAGIC * Store the results in a temporary view named **`q2Results`**
-- MAGIC    
-- MAGIC A properly completed solution should return a DataFrame that looks similar to this:
-- MAGIC 
-- MAGIC | firstName | lastName | lateFee |
-- MAGIC |---------- |----------| ------- |
-- MAGIC |Lance|DaCosta |true|

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW q2Results AS
  SELECT firstName, lastName,
  EXISTS(expenses, card -> TO_DATE(card.paymentDue) > TO_DATE(card.lastPayment)) AS lateFee
  FROM finances;

SELECT * FROM q2Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 3: Reduce
-- MAGIC ### Summary
-- MAGIC Use the **`REDUCE`** function to produce a query on the table **`charges`** that calculates total charges in dollars and total charges in Japanese Yen. 
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query that achieves the following: 
-- MAGIC * Uses the **`REDUCE`** function to calculate the total charges in US Dollars (given)
-- MAGIC * Uses the **`REDUCE`** function to convert the total charges to Japanese Yen using a conversion rate where 1 USD = 107.26 JPY 
-- MAGIC * Stores the results in a temporary table named **`q3Results`**
-- MAGIC    
-- MAGIC **NOTE:** In the `REDUCE` function, the accumulator must be of the same type as the input. You will have to `CAST` the accumulator as a `DOUBLE` to use this function with this data. 
-- MAGIC example: `CAST (0 AS DOUBLE`)
-- MAGIC 
-- MAGIC A properly completed solution should return a DataFrame that looks similar to this:
-- MAGIC 
-- MAGIC | firstName | lastName | allCharges | totalDollars | totalYen |
-- MAGIC |---------- |----------| ------- | --------| ----------|
-- MAGIC |Lance|DaCosta |["2222.46", "5976.76", "2543.55"]|10742.77|1152269.51|

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW q3Results AS
  SELECT firstName, lastName, allCharges, 
  REDUCE (allCharges, CAST(0 AS DOUBLE), (charge, acc) -> charge + acc) AS totalDollars,
  REDUCE (allCharges, CAST(0 AS DOUBLE), (charge, acc) -> charge + acc, acc -> ROUND(acc * 107.26, 2)) AS totalYen
  FROM charges;

SELECT * FROM q3Results


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
