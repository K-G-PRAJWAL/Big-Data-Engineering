-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aggregating and summarizing data
-- MAGIC 
-- MAGIC Now let's look at some powerful functions we can use to aggregate and summarize data. In this notebook, we will continue to work with hihger-order functions; this time we will apply them to arrays containing numerical data. Also, we will work with additional functions in Spark SQL  that can be helpful when presenting data. 
-- MAGIC 
-- MAGIC In this notebook, you will: 
-- MAGIC * Apply higher-order functions to numeric data
-- MAGIC * Use a `PIVOT` command to create Pivot tables
-- MAGIC * Use `ROLLUP` and `CUBE` modifiers to generate subtotals
-- MAGIC * Use window functions to perform operations on a group of rows
-- MAGIC * Use Databricks visualization tools to visualize and share data
-- MAGIC 
-- MAGIC Run the cell below to set up our classroom environment.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Higher-order functions and numerical data
-- MAGIC 
-- MAGIC Each of the higher-order functions we worked with in the last lesson can also be used with numerical data. In this lesson, we demonstrate how each of the functions in the previous lesson work with numeric data, as well as explore some powerful new higher-order functions.
-- MAGIC 
-- MAGIC Run the next two cells to create and describe the table we will be working with. You may recognize this table from a previous lesson. Recall that it contains data measuring environmental variability in a collection of data centers. The table `DeviceData` contains the `temps` and `co2Level` arrays we use to demonstrate higher-order functions. 

-- COMMAND ----------

DROP TABLE IF EXISTS DCDataRaw;
CREATE TABLE DCDataRaw
USING parquet                           
OPTIONS (
    PATH "/mnt/training/iot-devices/data-centers/2019-q2-q3"
    );
    
CREATE TABLE IF NOT EXISTS DeviceData     
USING parquet                               
WITH ExplodeSource                        
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
  value.co2_level co2Level
  
FROM ExplodeSource;

-- COMMAND ----------

DESCRIBE DeviceData;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Preview data
-- MAGIC 
-- MAGIC Let's take a look a sample of the data so that we con better understand the array values. 

-- COMMAND ----------

SELECT 
  temps, 
  co2_level
FROM DeviceData
TABLESAMPLE (1 ROWS)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Filter
-- MAGIC 
-- MAGIC Filter operates on arrays containing numeric data just the same as those with text data. In this case, let's imagine that we want to collect all temperatures above a given threshold. Run the cell below to view the example. 

-- COMMAND ----------

SELECT 
  temps, 
  FILTER(temps, t -> t > 18) highTemps
FROM DeviceData


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exists
-- MAGIC Exists operates on arrays containing numeric data just the same as those with text data. Let's say that we want to flag the records whose temperatures have exceeded a given value. Run the cell below to view the example. 

-- COMMAND ----------

SELECT 
  temps, 
  EXISTS(temps, t -> t > 23) highTempsFlag
FROM DeviceData

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Transform
-- MAGIC 
-- MAGIC When using `TRANSFORM` with numeric data, we can apply any built-in function meant to work with a single value or we can name our own set of operations to be applied to each value in the array. This data includes temperature readings taken in Celsius. Each row contains an array of 12 temperature readings. We can use `TRANSFORM` to convert each element of each array to Fahrenheit. To convert from Celsius to Fahrenheit, multiply the temperature in Celsius by 9, divide by 5, and then add 32.
-- MAGIC 
-- MAGIC Let's dissect the code below to better understand the function: 
-- MAGIC 
-- MAGIC `TRANSFORM(temps, t -> ((t * 9) div 5) + 32 ) temps_F`
-- MAGIC 
-- MAGIC **`TRANSFORM`** : the name of the higher-order function <br>
-- MAGIC **`temps`** : the name of our input array <br>
-- MAGIC **`t`** : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time. <br>
-- MAGIC **`->`** :  Indicates the start of the function <br>
-- MAGIC **` ((t * 9) div 5) + 32`** : This is the function. For each value in the input array, the value is multipled by 9 and then divided by 5. Then, we add 32. This is the formula for converting from Celcius to Fahrenheit.
-- MAGIC Recall that TRANSFORM takes an array, an iterator, and an anonymous function as input. In the code below, temps is the column that holds the array and t is the iterator that cycles through the list. The anonymous function ((t * 9) div 5) + 32 will be applied to each value in the list.
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We use [the function](https://spark.apache.org/docs/latest/api/sql/#div) **`div`** to divide one expression by another without including decimal values. This is strictly for neatness in this example. For operations where precision counts, you would use the `/` operator, which performs floating point division. 

-- COMMAND ----------

SELECT 
  temps temps_C,
  TRANSFORM (temps, t -> ((t * 9) div 5) + 32 ) temps_F
FROM DeviceData;


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ### Reduce 
-- MAGIC `REDUCE` is more advanced than `TRANSFORM`; it takes two lambda functions. You can use it to reduce the elements of an array to a single value by merging the elements into a buffer, and applying a finishing function on the final buffer. 
-- MAGIC 
-- MAGIC We will use the reduce function to find an average value, by day, for our CO<sub>2</sub> readings. Take a closer look at the individual pieces of the `REDUCE` function by reviewing the list below. 
-- MAGIC 
-- MAGIC `REDUCE(co2_level, 0, (c, acc) -> c + acc, acc ->(acc div size(co2_level)))`
-- MAGIC 
-- MAGIC 
-- MAGIC **`co2_level`** is the input array<br>
-- MAGIC **`0`** is the starting point for the buffer. Remember, we have to hold a temporary buffer value each time a new value is added to from the array; we start at zero in this case to get an accurate sum of the values in the list. <br>
-- MAGIC **`(c, acc)`** is the list of arguments we'll use for this function. It may be helpful to think of `acc` as the buffer value and `c` as the value that gets added to the buffer.<br>
-- MAGIC **`c + acc`** is the buffer function. As the function iterates over the list, it holds the total (`acc`) and adds the next value in the list (`c`). <br>
-- MAGIC **`acc div size(co2_level)`** is the finishing function. Once we have the sum of all numbers in the array, we divide by the number of elements to find the average. <br>
-- MAGIC 
-- MAGIC This view will be useful in subsequent exercises, so we’ll create a temporary view in this step so that we can access it later on. 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW Co2LevelsTemporary
AS
  SELECT
    dc_id, 
    device_type,
    co2_Level,
    REDUCE(co2_Level, 0, (c, acc) -> c + acc, acc ->(acc div size(co2_Level))) as averageCo2Level
  
  FROM DeviceData  
  SORT BY averageCo2Level DESC;

SELECT * FROM Co2LevelsTemporary

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Other higher-order functions
-- MAGIC There are many built-in functions designed to work with array type data and well as other higher-order functions to explore. You can import [this notebook](https://docs.databricks.com/_static/notebooks/apache-spark-2.4-functions.html?_ga=2.12496948.1216795462.1586360468-278368669.1586265166) for a list of examples. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Pivot tables: Example 1
-- MAGIC Pivot tables are supported in Spark SQL. A pivot table allows you to transform rows into columns and group by any data field. Let's take a closer look at our query. 
-- MAGIC 
-- MAGIC **`SELECT * FROM ()`**: The `SELECT` statement inside the parentheses in the input for this table. Note that it takes two columns from the view `Co2LevelsTemporary` <br>
-- MAGIC **`PIVOT`**: The first argument in the clause is an aggregate function and the column to be aggregated. Then, we specify the pivot column in the `FOR` subclause. The `IN` operator contains the pivot column values. <br>

-- COMMAND ----------

SELECT * FROM (
  SELECT device_type, averageCo2Level 
  FROM Co2LevelsTemporary
)
PIVOT (
  ROUND(AVG(averageCo2Level), 2) avg_co2 
  FOR device_type IN ('sensor-ipad', 'sensor-inest', 
    'sensor-istick', 'sensor-igauge')
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pivot Tables: Example 2
-- MAGIC 
-- MAGIC In this example, we again pull data from our larger table `DeviceData`. Within the subquery, we create the `month` column and use the `REDUCE` function to create the `averageCo2Level` column. 
-- MAGIC 
-- MAGIC In the pivot, we take the average of of the `averageCo2Level` values grouped by month. Notice that we rename the month columns from their number to the english abbreviations. 
-- MAGIC 
-- MAGIC Learn more about pivot tables in this [blog post](https://databricks.com/blog/2018/11/01/sql-pivot-converting-rows-to-columns.html).

-- COMMAND ----------

SELECT
  *
FROM
  (
    SELECT
      month(date) month,
      REDUCE(co2_Level, 0, (c, acc) -> c + acc, acc ->(acc div size(co2_Level))) averageCo2Level
    FROM
      DeviceData
  ) PIVOT (
    avg(averageCo2Level) avg FOR month IN (7 JUL, 8 AUG, 9 SEPT, 10 OCT, 11 NOV)
  )

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Rollups
-- MAGIC 
-- MAGIC Rollups are operators used with the `GROUP BY` clause. They allow you to summarize data based on the columns passed to the `ROLLUP` operator. 
-- MAGIC 
-- MAGIC This results of this query include average CO<sub>2</sub> levels, grouped by `dc_id` and `device_type`. Rollups are creating subtotals for a specific group of data. The subtotals introduce new rows, and the new rows will contain `NULL` values for everything other than the calculated subtotal. 
-- MAGIC 
-- MAGIC We can alter this by using the `COALESCE()` function introduced in a previous lesson. 

-- COMMAND ----------

SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(averageCo2Level))  AS avgCo2Level
FROM Co2LevelsTemporary
GROUP BY ROLLUP (dc_id, device_type)
ORDER BY dc_id, device_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cube
-- MAGIC `CUBE` is also an operator used with the `GROUP BY` clause. Similar to `ROLLUP`, you can use `CUBE` to generate summary values for sub-elements grouped by column value. `CUBE` is different than `ROLLUP` in that it will also generate subtotals for all combinations of grouping columns specified in the `GROUP BY` clause. 
-- MAGIC 
-- MAGIC Notice that the output for the example below shows some of additional values generated in this query. Data from `"All data centers"` has been aggregated across device types for all centers. 

-- COMMAND ----------

SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(averageCo2Level))  AS avgCo2Level
FROM Co2LevelsTemporary
GROUP BY CUBE (dc_id, device_type)
ORDER BY dc_id, device_type;

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
