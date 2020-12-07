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

-- MAGIC %run ../Utilities/03-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 1: Joins
-- MAGIC ### Summary 
-- MAGIC Perform a join of two tables **`purchases`** and **`prices`**.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query that acheives the following: 
-- MAGIC * Performs an inner join on two tables **`purchases`** and **`prices`** on the column **`itemId`**
-- MAGIC * Includes **only** 3 columns from the **`purchases`** table: **`transactionId`**, **`itemId`**, and **`value`**

-- COMMAND ----------

--TODO  Answer 1


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 2: Combine Tables
-- MAGIC ### Summary
-- MAGIC Perform an outer join on two tables **`discounts`** and **`products`** store the results into **`q2Results`** view.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query that achieves the following: 
-- MAGIC * Performs an outer join on table **`discounts`** and **`products`** on the **`itemName`** column
-- MAGIC * Ensures that the joined view only includes **one** **`itemName`** column that comes from the **`products`** table
-- MAGIC 
-- MAGIC 
-- MAGIC The final schema and DataFrame should contain the following columns, though not necessarily in this order:
-- MAGIC 
-- MAGIC |column |type |
-- MAGIC |---|---|
-- MAGIC |itemName | string|
-- MAGIC |discountId | integer|
-- MAGIC |discountCode | string|
-- MAGIC |price | double|
-- MAGIC |active | boolean|
-- MAGIC |itemId | integer|
-- MAGIC |amount | integer|

-- COMMAND ----------

-- TODO  Answer 2


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 3: Cross Join Tables
-- MAGIC ### Summary
-- MAGIC Perform a cross join on two tables **`stores`** and **`articles`**.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC 
-- MAGIC * Perform a cross join on two tables **`stores`** and **`articles`**

-- COMMAND ----------

-- TODO  Answer 3


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
