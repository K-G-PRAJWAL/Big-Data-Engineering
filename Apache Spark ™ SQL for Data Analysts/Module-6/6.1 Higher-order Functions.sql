-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Higher-order functions
-- MAGIC 
-- MAGIC Higher order functions in Spark SQL allow you to work directly with complex data types. When working with hierarchical data, as we were in the previous lesson and lab, records are frequently stored as array or map type objects. This lesson will demonstrate how to use higher-order functions to transform, filter, and flag array data while preserving the original structure. In this notebook, we work strictly with arrays of strings; in the subsquent notebook, you will work with more functions and numerical data. Skilled application of these functions can make your work with this kind of data faster, more powerful, and more reliable. 
-- MAGIC 
-- MAGIC In this notebook, you will: 
-- MAGIC * Apply higher-order functions (`TRANSFORM`, `FILTER`, `EXISTS`) to arrays of strings 
-- MAGIC 
-- MAGIC Run the following queries to learn about how to work with higher-order functions.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Getting Started
-- MAGIC Run the cell below to set up your classroom environment

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Working with Text Data
-- MAGIC 
-- MAGIC We can use higher-order functions to easily work with arrays of text data. The exercises in this section are meant to demonstrate the `TRANSFORM`, `FILTER`, and `EXISTS` functions to manipulate data and create flags for when a value does or does not exist. 
-- MAGIC 
-- MAGIC These examples use data collected about Databricks blog posts. Run the cell below to create the table. Then, run the next cell to view the schema. 
-- MAGIC 
-- MAGIC In this data set, the `authors` and `categories` columns are both ArrayType; we'll be using these columns with higher-order functions. 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DatabricksBlog
  USING json
  OPTIONS (
    path "dbfs:/mnt/training/databricks-blog.json",
    inferSchema "true"
  )

-- COMMAND ----------

DESCRIBE DatabricksBlog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Filter
-- MAGIC 
-- MAGIC [Filter](https://spark.apache.org/docs/latest/api/sql/#filter) allows us to create a new column based on whether or not values in an array meet a certain condition. Let's say we want to remove the category `"Engineering Blog"` from all records in our `categories` column. I can use the `FILTER` function to create a new column that excludes that value from the each array. 
-- MAGIC 
-- MAGIC Let's dissect this line of code to better understand the function:
-- MAGIC 
-- MAGIC `FILTER (categories, category -> category <> "Engineering Blog") woEngineering`
-- MAGIC 
-- MAGIC **`FILTER`** : the name of the higher-order function <br>
-- MAGIC **`categories`** : the name of our input array <br>
-- MAGIC **`category`** : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time.<br>
-- MAGIC **`->`** :  Indicates the start of a function <br>
-- MAGIC **`category <> "Engineering Blog"`** : This is the function. Each value is checked to see if it **is different** than the value `"Engineering Blog"`. If it is, it gets filtered into the new column, `woEnginieering`

-- COMMAND ----------

SELECT
  categories,
  FILTER (categories, category -> category <> "Engineering Blog") woEngineering
FROM DatabricksBlog


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Filter, subqueries, and `WHERE`
-- MAGIC 
-- MAGIC You may write a filter that produces a lot of empty arrays in the created column. When that happens, it can be useful to use a `WHERE` clause to show only non-empty array values in the returned column. 
-- MAGIC 
-- MAGIC In this example, we accomplish that by using a subquery. A **subquery** in SQL is a query within a query. They are useful for performing an operations in multiple steps. In this case, we're using it to create the named column that we will use with a `WHERE` clause. 

-- COMMAND ----------

SELECT
  *
FROM
  (
    SELECT
      authors, title,
      FILTER(categories, category -> category = "Engineering Blog") AS blogType
    FROM
      DatabricksBlog
  )
WHERE
  size(blogType) > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Exists
-- MAGIC 
-- MAGIC [Exists](https://spark.apache.org/docs/latest/api/sql/#exists) tests whether a statement is true for one or more elements in an array. Let's say we want to flag all blog posts with `"Company Blog"` in the categories field. I can use the `EXISTS` function to mark which entries include that category.
-- MAGIC 
-- MAGIC Let's dissect this line of code to better understand the function: 
-- MAGIC 
-- MAGIC `EXISTS (categories, c -> c = "Company Blog") companyFlag`
-- MAGIC 
-- MAGIC **`EXISTS`** : the name of the higher-order function <br>
-- MAGIC **`categories`** : the name of our input array <br>
-- MAGIC **`c`** : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time. Note that we're using the same kind as references as in the previous command, but we name the iterator with a single letter<br>
-- MAGIC **`->`** :  Indicates the start of a function <br>
-- MAGIC **`c = "Engineering Blog"`** : This is the function. Each value is checked to see if it **is the same as** the value `"Company Blog"`. If it is, it gets flagged into the new column, `companyFlag`

-- COMMAND ----------

SELECT
  categories,
  EXISTS (categories, c -> c = "Company Blog") companyFlag
FROM DatabricksBlog


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transform
-- MAGIC 
-- MAGIC [Transform](https://spark.apache.org/docs/latest/api/sql/#transform) uses the provided function to transform all elements of an array. SQL's built-in functions are designed to operate on a single, simple data type within a cell. They cannot process array values. Transform can be particularly useful when you want to apply an existing function to each element in an array. In this case, we want to rewrite all of the names in the `categories` column in lowercase. 
-- MAGIC 
-- MAGIC Let's dissect this line of code to better understand the function: 
-- MAGIC 
-- MAGIC `TRANSFORM(categories, cat -> LOWER(cat)) lwrCategories`
-- MAGIC 
-- MAGIC **`TRANSFORM`** : the name of the higher-order function <br>
-- MAGIC **`categories`** : the name of our input array <br>
-- MAGIC **`cat`** : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time. Note that we're using the same kind as references as in the previous command, but we name the iterator with a new variable<br>
-- MAGIC **`->`** :  Indicates the start of a function <br>
-- MAGIC **`LOWER(cat)`** : This is the function. For each value in the input array, the built-in function `LOWER()` is applied to transform the word to lowercase. 

-- COMMAND ----------

SELECT 
  TRANSFORM(categories, cat -> LOWER(cat)) lwrCategories
FROM DatabricksBlog

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
