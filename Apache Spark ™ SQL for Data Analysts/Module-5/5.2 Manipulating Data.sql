-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manipulating Data
-- MAGIC 
-- MAGIC In this notebook, you will be working with the online retail sales data that you worked with in the Module 3 Lab. This time, you will work with the columns of data that contain `NULL` values and a non-standard date format. 
-- MAGIC 
-- MAGIC Run the following queries to learn about how to work with and manage null values and timestamps in Spark SQL. In this notebook, you will:
-- MAGIC 
-- MAGIC * Sample a table
-- MAGIC * Access individual values from an array
-- MAGIC * Reformat values using a padding function
-- MAGIC * Concatenate values to match a standard format
-- MAGIC * Access parts of a `DateType` value like the month, day, or year

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Getting Started
-- MAGIC 
-- MAGIC Run the cell below to set up your classroom environment. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create table
-- MAGIC 
-- MAGIC Our data is stored as a csv. The optional arguments show the path to the data and store the first row as a header. 

-- COMMAND ----------

DROP TABLE IF EXISTS outdoorProductsRaw;
CREATE TABLE outdoorProductsRaw USING csv OPTIONS (
  path "/mnt/training/online_retail/data-001/data.csv",
  header "true"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Describe
-- MAGIC 
-- MAGIC Recall that the `DESCRIBE` command tells us about the schema of the table. Notice that all of our columns are string values. 

-- COMMAND ----------

DESCRIBE outdoorProductsRaw

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Sample the table
-- MAGIC In the previous reading, you accessed a random sample of rows from a table using the `RAND()` function and `LIMIT` keyword. While this is a common way to retrieve a sample with other SQL dialects, Spark SQL includes a built-in function that you may want to use instead. 
-- MAGIC 
-- MAGIC The function, `TABLESAMPLE`, allows you to return a number of rows or a certain percentage of the data. In the cell directly below this one, we show that `TABLESAMPLE` can be used to access a specific number of rows. In the following cell, we show that it can be used to access a given percentage of the data. Please note, however, any table display is limited to 1,000 rows. If the percentage of data you request returns more than 1,000 rows, only the first 1000 will show. 
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The sample that displays 2 percent of the table is also ordered by the `InvoiceDate`. This shows off a formatting issue in the date column that we will have to fix later on. Take a moment and see if you can predict how we might need to change in the way the `InvoiceDate` is written. 

-- COMMAND ----------

SELECT * FROM outdoorProductsRaw TABLESAMPLE (5 ROWS)

-- COMMAND ----------

SELECT * FROM outdoorProductsRaw TABLESAMPLE (2 PERCENT) ORDER BY InvoiceDate 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check for null values
-- MAGIC 
-- MAGIC Run this cell to see the number of `NULL` values in the `Description` column of our table. 

-- COMMAND ----------

SELECT count(*) FROM outdoorProductsRaw WHERE Description IS NULL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a temporary view
-- MAGIC 
-- MAGIC The next cell creates the temporary view `outdoorProducts`. By now, you should be familiar with how to create (or replace) a temporary view. There are a few new commands to notice in this particular command. 
-- MAGIC 
-- MAGIC This is where we will start to work with the problematic date formatting mentioned previously. Did you notice the inconsistency in your displays? 
-- MAGIC 
-- MAGIC Our dates do not have a standard number of digits for months and years. For example, `12/1/11` has a two-digit month and one-digit day, while `1/10/11` has an one-digit month and two-digit day. It's easy enough to specify a format to convert a string to a date, but the format must be consistent throughout the table. We will begin to attempt a fix for this problem by simply separating all of the components of the date and dropping the time value entirely. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Code breakdown 
-- MAGIC 
-- MAGIC **`COALESCE`** - This command is popular among many different SQL dialects. We can use it to replace `NULL` values. For all `NULL` values in the `Description` column, `COALESCE()` will replace the null with a value you include in the function. In this case, the value is `"Misc"`. For more information about `COALESCE`, check [the documentation](https://spark.apache.org/docs/latest/api/sql/index.html#coalesce).
-- MAGIC 
-- MAGIC **`SPLIT`** - This command splits a string value around a specified character and returns an **array**. An array is a list of values that you can access by position. In this case, the forward slash ("/") is the character we use to split the data. The first value in the array is the month. This list is **zero-indexed** for the index of the first position is **0**. Since we want to pull out the first value as the month, we indicate the value like this: `SPLIT(InvoiceDate, "/")[0]` and rename the column **`month`**. The day is the second value and its index is 1. 
-- MAGIC 
-- MAGIC The third `SPLIT` is different. Remember that our `InvoiceDate` column is a string that includes a date and time. Each part of the date is seperated by a forward slash, but between the date and the time, there is only a space. **`Line 10`** contains a **nested** `SPLIT` function that splits the string on a space delimiter. 
-- MAGIC 
-- MAGIC `SPLIT(InvoiceDate, " ")[0]` --> Drops the time from the string and leaves the date intact. Then, we split that value on the forward slash delimiter. We access the year at index 2. Learn more about the `SPLIT` function by accessing [the documentation](https://spark.apache.org/docs/latest/api/sql/#split).

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW outdoorProducts AS
SELECT
  InvoiceNo,
  StockCode,
  COALESCE(Description, "Misc") AS Description,
  Quantity,
  SPLIT(InvoiceDate, "/")[0] month,
  SPLIT(InvoiceDate, "/")[1] day,
  SPLIT(SPLIT(InvoiceDate, " ")[0], "/")[2] year,
  UnitPrice,
  Country
FROM
  outdoorProductsRaw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check "Misc" values
-- MAGIC 
-- MAGIC We perform a quick sanity check here to demonstrate that all of the `NULL` values in Description have been replaced with the string `"Misc"`. 

-- COMMAND ----------

SELECT count(*) FROM outdoorProducts WHERE Description = "Misc" 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a new table
-- MAGIC 
-- MAGIC Now, we can write a new table with a consistently formatted date string. Notice that this table creation statement has a CTE inside of it. Recall that the CTE starts with a `WITH` clause. 
-- MAGIC 
-- MAGIC Notice the `LPAD()` functions on lines 11 and 12. [This function](https://spark.apache.org/docs/latest/api/sql/#lpad) inserts characters to the left of a string until the string reachers a certain length. In this example, we use `LPAD` to insert a zero to the left of any value in the month or day column that **is not** two digits. For values that are two digits, `LPAD` does nothing. 
-- MAGIC 
-- MAGIC We use the `padStrings` CTE to standardize the length of the individual date components. When we query the CTE, we use the `CONCAT_WS()` function to put the date string back together.  [This function](https://spark.apache.org/docs/latest/api/sql/#concat_ws) returns a concatenated string with a specified separator. In this example, we concatenate values from the month, date, and year columns, and specify that each value should be separated by a forward slash ("/"). 

-- COMMAND ----------

DROP TABLE IF EXISTS standardDate;
CREATE TABLE standardDate

WITH padStrings AS
(
SELECT 
  InvoiceNo,
  StockCode,
  Description,
  Quantity, 
  LPAD(month, 2, 0) AS month,
  LPAD(day, 2, 0) AS day,
  year,
  UnitPrice, 
  Country
FROM outdoorProducts
)
SELECT 
 InvoiceNo,
  StockCode,
  Description,
  Quantity, 
  concat_ws("/", month, day, year) sDate,
  UnitPrice,
  Country
FROM padStrings;






-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Table check
-- MAGIC When we view our new table, we can see that the date field shows two digits each for the month, day, and year. 

-- COMMAND ----------

SELECT * FROM standardDate LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check schema
-- MAGIC 
-- MAGIC Oops! All of our values are still strings. The date field would be much more useful as a `DateType`. 

-- COMMAND ----------

DESCRIBE standardDate;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Change to DateType
-- MAGIC 
-- MAGIC In the next cell, we create a new temporary view that converts the value to a date. The optional argument `MM/dd/yy` indicates the meaning of each part of the date. You can find a complete guide to Spark SQL's Datetime Patterns [here](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html).
-- MAGIC 
-- MAGIC Also, we cast the `UnitPrice` as a `DOUBLE` so that we can treat it as a number. 

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW salesDateFormatted AS
SELECT
  InvoiceNo,
  StockCode,
  to_date(sDate, "MM/dd/yy") date,
  Quantity,
  CAST(UnitPrice AS DOUBLE)
FROM
  standardDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualize Data
-- MAGIC 
-- MAGIC We can extract the day of the week and figure out the total quantitybar of items sold on each day. You can create a quick visual by clicking on the chart icon and creating a bar chart where the key is the `day` and the values are the `quantity`. 
-- MAGIC 
-- MAGIC We use the `date_format()` function to map the day to a day of the week. [This function](https://spark.apache.org/docs/latest/api/sql/#date_format) converts a `timestamp` to a `string` in the format specified. For this command, the `"E"` specifies that we want the output to be the day of the week. 

-- COMMAND ----------

SELECT
  date_format(date, "E") day,
  SUM(quantity) totalQuantity
FROM
  salesDateFormatted
GROUP BY (day)
ORDER BY day


-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
