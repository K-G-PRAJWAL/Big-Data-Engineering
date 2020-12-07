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

-- MAGIC %run ../Utilities/05-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 1: Min Function
-- MAGIC ### Summary
-- MAGIC Compute the minimum value from the **`Amount`** field for each unique value in the **`TrueFalse`** field in the table **`revenue1`**.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query that achieves the following: 
-- MAGIC * Computes the number of **`true`** and **`false`** records in the **`TrueFalse`** field from the table **`revenue1`**
-- MAGIC * Renames the new column to **`count`**
-- MAGIC * Store the records in a temporary view named  **`q1Results`** with the following schema:
-- MAGIC 
-- MAGIC | column | type |
-- MAGIC |--------|--------|
-- MAGIC | TrueFalse | boolean |
-- MAGIC | MinAmount | int |
-- MAGIC 
-- MAGIC A properly completed solution should produce a view similar to this sample output:
-- MAGIC 
-- MAGIC |TrueFalse|         count |
-- MAGIC |---------|------------------|
-- MAGIC |     true|        4956|
-- MAGIC |    false|        5044|

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW q1Results AS
  SELECT count(*) AS count FROM revenue1
  GROUP BY TrueFalse;

SELECT * FROM q1Results;
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 2: Max Function
-- MAGIC ### Summary
-- MAGIC Compute the maximum value from the **`Amount`** field for each unique value in the **`TrueFalse`** field in the table **`revenue2`**.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC * Computes the maximum **`Amount`** for **`True`** records and **`False`** records from the **`TrueFalse`** field from the table **`revenue2`**
-- MAGIC * Renames the new column to **`maxAmount`**
-- MAGIC * Store the records in a temporary view named  **`q2Results`** with the following schema:
-- MAGIC    
-- MAGIC | column | type |
-- MAGIC |--------|--------|
-- MAGIC | TrueFalse | boolean |
-- MAGIC | maxAmount | double |
-- MAGIC 
-- MAGIC A properly completed solution should produce a DataFrame similar to this sample output:
-- MAGIC 
-- MAGIC |TrueFalse|         MaxAmount|
-- MAGIC |---------|------------------|
-- MAGIC |     true|        2243937.93|
-- MAGIC |    false|2559457.1799999997|

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW q2results AS
  SELECT TrueFalse, max(Amount) AS MaxAmount 
  FROM revenue2
  GROUP BY TrueFalse;
  
SELECT * FROM q2Results;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 3: Avg Function
-- MAGIC ### Summary
-- MAGIC Compute the average of the **`Amount`** field for each unique value in the **`TrueFalse`** field in the table **`revenue3`**.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC 
-- MAGIC * Computes the average of **`Amount`** for **`True`** records and **`False`** records from the **`TrueFalse`** field in the table **`revenue3`**.
-- MAGIC * Renames the new column to **`avgAmount`**
-- MAGIC * Store the records in a temporary view named  **`q3Results`** with the following schema:
-- MAGIC 
-- MAGIC | column | type |
-- MAGIC |--------|--------|
-- MAGIC | TrueFalse | boolean |
-- MAGIC | avgAmount | double |
-- MAGIC 
-- MAGIC A properly completed solution should produce a DataFrame similar to this sample output:
-- MAGIC 
-- MAGIC |TrueFalse|         AvgAmount|
-- MAGIC |---------|------------------|
-- MAGIC |     true|        2243937.93|
-- MAGIC |    false|2559457.1799999997|

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TEMPORARY VIEW q3Results AS
  SELECT TrueFalse, avg(Amount) AS AvgAmount 
  FROM revenue3
  GROUP BY TrueFalse;

SELECT * FROM q3Results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 4: Pivot
-- MAGIC ### Summary
-- MAGIC Calculate the total **`Amount`** for **`YesNo`** values of **true** and **false** in 2002 and 2003 from the table **`revenue4`**.
-- MAGIC     
-- MAGIC ### Steps to complete
-- MAGIC * Casts the **`UTCTime`** field to Timestamp and names the new column **`Date`**
-- MAGIC * Extracts a **`Year`** column from the **`Date`** column
-- MAGIC * Filters for years greater than 2001 and less than or equal to 2003
-- MAGIC * Groups by **`YesNo`** and creates a pivot table to get the total **`Amount`** for each year and each value in **`YesNo`**
-- MAGIC * Represents each total amount as a float rounded to two decimal places
-- MAGIC * Store the results into a temporary table named **`q4results`**
-- MAGIC    
-- MAGIC A properly completed solution should produce a view similar to this sample output:
-- MAGIC 
-- MAGIC |YesNo|    2002|    2003|
-- MAGIC |-----|--------|--------|
-- MAGIC | true| 61632.3| 8108.47|
-- MAGIC |false|44699.99|35062.22|

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TEMPORARY VIEW q4Results AS
  SELECT * 
  FROM (SELECT Year, YesNo, Amount
        FROM (SELECT year(CAST(UTCTime AS timestamp)) as Year,
                     YesNo,
                     Amount 
              FROM revenue4) 
        WHERE Year > 2001 AND Year <= 2003)
  PIVOT ( round( sum(Amount), 2) AS total FOR Year in (2002, 2003) );
  
  SELECT * FROM q4Results;
  
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 5: Null Values and Aggregates
-- MAGIC ### Summary
-- MAGIC Compute sums of **`amount`** grouped by **`aisle`** after dropping null values from **`products`** table.
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC 
-- MAGIC * Drops any rows that contain null values in either the **`itemId`** or the **`aisle`** column
-- MAGIC * Aggregates sums of the **`amount`** column grouped by **`aisle`**
-- MAGIC * Store the results into a temporary view named  **`q5Results`**

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW q5Results AS
  SELECT aisle, sum(amount) FROM products 
  WHERE (itemId IS NOT NULL AND aisle IS NOT NULL) 
  GROUP BY aisle;
  
SELECT * FROM q5Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 6: Generate Subtotals By Rollup
-- MAGIC ### Summary
-- MAGIC Compute averages of **`income`** grouped by **`itemName`** and **`month`** such that the results include averages across all months as well as a subtotal for an individual month from the **`sales`** table. 
-- MAGIC 
-- MAGIC ### Steps to complete
-- MAGIC 
-- MAGIC * Coalesces null values in the **`month`** column generated by the `ROLLUP` clause
-- MAGIC * Store the results into a temporary view named  **`q6Results`**
-- MAGIC 
-- MAGIC Your results should look something like this: 
-- MAGIC 
-- MAGIC | itemName| month | avgRevenue |
-- MAGIC | --------| ----- | ---------- |
-- MAGIC | Anim | 10 | 4794.16 |
-- MAGIC | Anim | 7 | 5551.31 |
-- MAGIC | Anim | All months | 5046.54 |
-- MAGIC | Aute | 4 | 4069.51 |
-- MAGIC | Aute | 7 | 3479.31 |
-- MAGIC | Aute | 8 | 6339.28 |
-- MAGIC | Aute | All months |  4489.41 |
-- MAGIC | ... | ... | ... | 

-- COMMAND ----------

--ANSWER
CREATE OR REPLACE TEMPORARY VIEW q6Results AS
  SELECT 
    COALESCE(itemName, "All items") AS itemName,
    COALESCE(month(date), "All months") AS month,
    ROUND(AVG(revenue), 2) as avgRevenue
  FROM sales
  GROUP BY ROLLUP (itemName, month(date))
  ORDER BY itemName, month;

SELECT * FROM q6Results;


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
