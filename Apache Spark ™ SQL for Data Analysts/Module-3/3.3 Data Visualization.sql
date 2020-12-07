-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Visualization with Databricks
-- MAGIC 
-- MAGIC In this lesson, you'll learn how to create and share data visualizations in Databricks. 
-- MAGIC 
-- MAGIC By the end of the lesson, you will be able to: 
-- MAGIC * Create a table with a specified schema
-- MAGIC * Cast a column as a timestamp and extract day, month, or year
-- MAGIC * Use in-notebook visualizations to see your data 
-- MAGIC 
-- MAGIC Step 1: Read through and run all the cells in this notebook. <br>
-- MAGIC Step 2: View the corresponding video to see instructions for visualizing and sharing data. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Getting Started
-- MAGIC 
-- MAGIC Run the following cell to connect your workspace to the appropriate data source. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Table
-- MAGIC 
-- MAGIC In the previous lesson, we created a table for you to start querying. In this lesson, you will create the table by reading directly from the data source and specifying a **schema**. A schema describes the structure of your data. It contains column names and the type of data in each column. All tables must have an associated schema; if you do not explicitly define one, Spark may be able to infer it.  
-- MAGIC 
-- MAGIC In the cell below, we define the schema as we create the table. This data has the following schema: 
-- MAGIC 
-- MAGIC |Column Name | Type |
-- MAGIC | ---------- | ---- |
-- MAGIC | userId | INT|
-- MAGIC | movieId | INT|
-- MAGIC | rating | FLOAT|
-- MAGIC | timeRecorded | INT|
-- MAGIC 
-- MAGIC Notice that it is defined right after the `CREATE TABLE` statement with the name of each column followed by the datatypes within the column. The whole group of columns is surround by parentheses and each individual column is separated by a comma. 

-- COMMAND ----------

DROP TABLE IF EXISTS movieRatings;
CREATE TABLE movieRatings (
  userId INT,
  movieId INT,
  rating FLOAT,
  timeRecorded INT
) USING csv OPTIONS (
  PATH "/mnt/training/movies/20m/ratings.csv",
  header "true"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preview the data
-- MAGIC 
-- MAGIC This table contains a little more than 20 million records of movies ratings submitted by users. Note that the timestamp is an integer value recorded in UTC time. 

-- COMMAND ----------

SELECT
  *
FROM
  movieRatings;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cast as timestamp
-- MAGIC 
-- MAGIC We use the `CAST()` function to show the timestamp as a human-readable time and date. 

-- COMMAND ----------

SELECT
  rating,
  CAST(timeRecorded as timestamp)
FROM
  movieRatings;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create temporary view
-- MAGIC We will create a temporary view that we can easily refer to the data we want to include in our visualization. For this data, we can investigate whether there are any patterns in the ratings when grouped by month. To do that, we use the `ROUND()` and `AVG()` functions to calculate the average rating and limit it to 3 decimal places. Then, extract the month from the `timeRecorded` column after casting it as a timestamp. The `AVG()` is calculated over the course of a month, as specified in the `GROUP BY` clause. 

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW ratingsByMonth AS
SELECT
  ROUND(AVG(rating), 3) AS avgRating,
  month(CAST(timeRecorded as timestamp)) AS month
FROM
  movieRatings
GROUP BY
  month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visualize the data
-- MAGIC 
-- MAGIC Run the next cell to see the view we just defined ordered by `avgRating` from least to greatest. The results will appear as a table. In the next section, you will receive video instruction that will show you how to display this table as a chart. 

-- COMMAND ----------

SELECT
  *
FROM
  ratingsByMonth
ORDER BY
  avgRating;

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Citation
-- MAGIC Access original data and backround information here: 
-- MAGIC 
-- MAGIC F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4, Article 19 (December 2015), 19 pages. DOI=<http://dx.doi.org/10.1145/2827872>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
