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

-- MAGIC %run ../Utilities/06-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 1: Array and Explode
-- MAGIC ### Summary
-- MAGIC Get a distinct list of authors who have contributed blog posts in the "Company Blog" category.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query that achieves the following: 
-- MAGIC - Explodes the **`authors`** field to create an **`author`** field that contains only one author per row in table **`databricksBlog`**.
-- MAGIC - Limits records to contain **only** unique authors
-- MAGIC - Filters records where **`categories`** include "Company Blog"
-- MAGIC - Store the results in a temporary view named  **`results`**
-- MAGIC 
-- MAGIC A properly completed solution should produce a view named **`results`** similar to this sample output:
-- MAGIC 
-- MAGIC |              author|        categories|
-- MAGIC |--------------------|----------------|
-- MAGIC |Anthony Joseph      |["Announcements", "Company Blog"]| 
-- MAGIC |Vida Ha        |["Company Blog", "Product"]| 
-- MAGIC |Nan Zhu (Chief Architect at Faimdata)    |["Company Blog", "Product"]| 

-- COMMAND ----------

--TODO  Answer 1



-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
