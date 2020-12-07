-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Partitioning Tables
-- MAGIC 
-- MAGIC You can affect query performance by partitioning data in your tables. Recall that we looked at some specific performance improvements (and downgrades) that can be caused by partitioning in Module 4, Spark Under the Hood. Partitioning data in a Spark SQL query creates a subdirectory of data according to a rule. For example, if I partition a set of data by year, all data in any given folder in the subdirectories for that table will have the same year.  That means, when is comes time to query the set and I include something like,<br> `WHERE year = 1990`, <br>
-- MAGIC Spark can avoid reading any data from folders that are **not** in the `1990` subfolder. 
-- MAGIC 
-- MAGIC In the next set of exercises,we will demonstrate examples of how to partition data, how to view table partitions, and how to use widgets to adjust your query parameters. 
-- MAGIC 
-- MAGIC Finally, we will demonstrate how to use window fucntions, which use a different sort of partitioning to compute values over a sub-section of a table. 
-- MAGIC 
-- MAGIC Run the cell below to set up your classroom environment. 

-- COMMAND ----------

-- MAGIC %run ../Includes/6-3-Setup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC In the example below, we create and use a table called `AvgTemps`. You may recognize this query from a previous notebook. This table includes temperature readings taken over entire days as well as the calculated value `avg_daily_temp_c`. 
-- MAGIC 
-- MAGIC Notice that this table has been `PARTITIONED BY` the column `device_type`. The result of this kind of partitioning is that the table is stored in separate files. This may speed up subsequent queries that can filter out certain partitions. These are <b>not</b> the same partitions  we refer to when discussing basic Spark architecture. 
-- MAGIC 
-- MAGIC 
-- MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The word **partition** is a bit overloaded in big data and distributed computing. That is, we have to pay careful attention to the context to understand what sort of partition is being discussed. In particular, when referring to Spark's internal architecture, partitions refer to the units of data that are physically distributed across a cluster. Managing this kind of partitioning is generally the job of a data engineer and is outside of the scope of this course. 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS AvgTemps
PARTITIONED BY (device_type)
AS
  SELECT
    dc_id,
    date,
    temps,
    REDUCE(temps, 0, (t, acc) -> t + acc, acc ->(acc div size(temps))) as avg_daily_temp_c,
    device_type
  FROM DeviceData;
  
SELECT * FROM AvgTemps;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Show partitions
-- MAGIC Use the command `SHOW PARTITIONS` to see how your data is partitioned. In this case, we can verify that the data has been partitioned according to device type. 

-- COMMAND ----------

SHOW PARTITIONS AvgTemps

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a widget 
-- MAGIC 
-- MAGIC Input widgets allow you to add parameters to your notebooks and dashboards. You can create and remove widgets, as well as retrieve values from them within a SQL query. Once created, they appear at the top of your notebook. You can design them to take user input as a:
-- MAGIC * dropdown: provide a list of options for the user to select from
-- MAGIC * text: user enters input as text
-- MAGIC * combobox: Combination of text and dropdown. User selects a value from a provided list or input one in the text box.
-- MAGIC * multiselect: Select one or more values from a list of provided values
-- MAGIC 
-- MAGIC Widgets are best for:
-- MAGIC * Building a notebook or dashboard that is re-executed with different parameters
-- MAGIC * Quickly exploring results of a single query with different parameters
-- MAGIC 
-- MAGIC Learn more about widgets [here](https://docs.databricks.com/notebooks/widgets.html).
-- MAGIC 
-- MAGIC We have already created a partitioned table, so we have one designated column meant to be used for easy data reads with filters. In this example, we'll use a widget to allow anyone viewing the notebook (or correspoding dashboard) the ability to filter by `device_type` in our table. 
-- MAGIC 
-- MAGIC In this example, we use a `DROPDOWN` so that the user can select among all available options. We name the widget `selectedDeviceType` and specify the `CHOICES` by getting a distinct list of all values in the `deviceType` column. 

-- COMMAND ----------

CREATE WIDGET DROPDOWN selectedDeviceType DEFAULT "sensor-inest" CHOICES
SELECT
  DISTINCT device_type
FROM
  DeviceData

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use the selected value in your query
-- MAGIC 
-- MAGIC We use a user-defined function, `getArgument()` to retrieve the current value selected in the widget. This functionality is available in the Databricks Runtime, but not open-source Spark. 
-- MAGIC 
-- MAGIC In the example below, we retrieve the selected value in the `WHERE` clause at the bottom of the query. Run the example. Then, change the value in the widget. Notice that the command below runs automatically. By default, cells that access input from a given widget will rerun automatically when the input value is changed. You can change default values using the ![settings](https://docs.databricks.com/_images/gear.png) icon on the right side of the widgets panel at the top of the notebook. 

-- COMMAND ----------

SELECT 
  device_type,
  ROUND(AVG(avg_daily_temp_c),4) AS avgTemp,
  ROUND(STD(avg_daily_temp_c), 2) AS stdTemp
FROM AvgTemps
WHERE device_type = getArgument("selectedDeviceType")
GROUP BY device_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Remove widget
-- MAGIC 
-- MAGIC You can remove widget with the following command by simply referencing it by name. 

-- COMMAND ----------

REMOVE WIDGET selectedDeviceType

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Window functions
-- MAGIC 
-- MAGIC Window functions calculate a return variable for every input row of a table based on a group of rows selected by the user, the frame. To use window functions, we need to mark that a function is used as a window by adding an `OVER` clause after a supported function in SQL. Within the `OVER` clause, you specify which rows are included in the frame associated with this window.
-- MAGIC 
-- MAGIC In the example, the function we will use is `AVG`. We define the Window Specification associated with this function with `OVER(PARTITION BY ...)`. The results show that the average monthly temperature is calculated for a data center on a given date. The `WHERE` clause at the end of this query is included to show a whole month of data from a single data center.

-- COMMAND ----------

SELECT 
  dc_id,
  month(date),
  avg_daily_temp_c,
  AVG(avg_daily_temp_c)
  OVER (PARTITION BY month(date), dc_id) AS avg_monthly_temp_c
FROM AvgTemps
WHERE month(date)="8" AND dc_id = "dc-102";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTEs with window functions
-- MAGIC 
-- MAGIC Here, we integrate a that same window functionand use a CTE to further manipulate values calculated in the common table expression. 

-- COMMAND ----------

WITH DiffChart AS
(
SELECT 
  dc_id,
  date,
  avg_daily_temp_c,
  AVG(avg_daily_temp_c)
  OVER (PARTITION BY month(date), dc_id) AS avg_monthly_temp_c  
FROM AvgTemps
)
SELECT 
  dc_id,
  date,
  avg_daily_temp_c,
  avg_monthly_temp_c,
  avg_daily_temp_c - ROUND(avg_monthly_temp_c) AS degree_diff
FROM DiffChart;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## Chart results 
-- MAGIC 
-- MAGIC First, use the graph tool to create the visualization. Configure your `Plot Options` to match the selections in the image below.
-- MAGIC 
-- MAGIC <img alt = "Plot Options" src="https://docs.google.com/drawings/d/e/2PACX-1vQQSthzh_aGvN5vgbAiSvxxtVCJHmH47sMAJkwIwnLEI5jSzOenVXjwnYYSQ-A68ROlwxZUWomQihs6/pub?w=720&h=486" style="float: left border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>
-- MAGIC 
-- MAGIC In the `Keys:` dialog box, add `date`.    
-- MAGIC In the `Values:` dialog box, add `avg_daily_temp_c` and `avg_monthly_temp_c`.    
-- MAGIC The `Aggregation` value should be set to `AVG` and the `Display type`set to `Line Chart`.  

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
