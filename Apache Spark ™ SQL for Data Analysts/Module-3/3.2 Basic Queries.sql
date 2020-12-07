-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic Queries with Spark SQL
-- MAGIC 
-- MAGIC Run the following queries to start working with Spark SQL. As you work, notice that Spark SQL syntax and patterns are the same as the SQL you would use in other modern database systems. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Getting Started
-- MAGIC 
-- MAGIC When you work with Databricks as part of an organization, it is likely that your workspace will be set up for you. In other words, you will be connected to various data stores and able to pull current data into your notebooks for analysis. In this course, you will use data provided by Databricks. The cell below runs a file that connects this workspace to data storage. You must run the cell below at the start of any new session. There's no need to worry about the output of this cell unless you get an error. It is simply preparing your workspace to be used with the this notebook. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Create table
-- MAGIC 
-- MAGIC We are going to be working with different files (and file formats) throughout this course. The first thing we need to do, in order to access the data through our SQL interface, is create a **table** from that data. 
-- MAGIC 
-- MAGIC A [Databricks table](https://docs.databricks.com/data/tables.html) is a collection of structured data. We will use Spark SQL to query tables.This table contains 10 million fictitious records that hold facts about people, like first and last names, date of birth, salary, etc. We're using the [Parquet](https://databricks.com/glossary/what-is-parquet) file format, which is commonly used in many big data workloads. We will talk more about various file formats later in this course.  Run the code below to access the table we'll use for the first part of this lesson. 
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We will embed links to documentation about Databricks, Spark, and Spark SQL throughout this course. It's important to be able to read an access documentation for any system. We encourage you to get comfortable with the docs and learn more by reading through the provided links.

-- COMMAND ----------

DROP TABLE IF EXISTS People10M;
CREATE TABLE People10M
USING parquet
OPTIONS (
path "/mnt/training/dataframes/people-10m.parquet",
header "true");

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Querying tables
-- MAGIC In the first part of this lesson,
-- MAGIC we 'll be using a table that has been defined for you, `People10M`. This table contains 10 million fictitious records. 
-- MAGIC 
-- MAGIC We start with a simple `SELECT` statement to get a view of the data.
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For now, you can think of a **table** much as you would a spreadsheet with named columns. The actual data is a file in an object store. When we define a table like the one in this exercise, it becomes available to anyone who has access to this Databricks workspace.  We will view
-- MAGIC and work with this table programmatically,but you can also preview it using the `Data` tab in the sidebar on the left side of the screen.

-- COMMAND ----------

SELECT * FROM People10M;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC We can view the schema for this table by using the `DESCRIBE` function.
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The **schema** is a list that defines the columns in a table and the datatypes within those columns. 

-- COMMAND ----------

DESCRIBE People10M;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Displaying query results
-- MAGIC 
-- MAGIC Any query that starts with a `SELECT` statement automatically displays the results below. We can use a `WHERE` clause to limit the results to those that meet a given condition or set of conditions. 
-- MAGIC 
-- MAGIC For the next query, we limit the result colums to `firstName`, `middleName`, `lastName`, and `birthdate`. We use a `WHERE` clause at the end to identify that we want to limit the result set to people born after 1990 whose gender is listed as `F`. 
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Since `birthDate` is a timestamp type, we can extract the year of birth using the function `YEAR()`

-- COMMAND ----------

SELECT
  firstName,
  middleName,
  lastName,
  birthDate
FROM
  People10M
WHERE
  year(birthDate) > 1990
  AND gender = 'F'

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Math
-- MAGIC 
-- MAGIC Spark SQL includes many <a href="https://spark.apache.org/docs/latest/api/sql/" target="_blank">built-in functions</a> that are also used in standard SQL. We can use them to create new columns based on a rule. In this case, we use a simple math function to calculate 20% of a person's listed. We use the keyword `AS` to rename the new column `savings`. 
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Many financial planning experts agree that 20% of a person's income should go into  savings. 

-- COMMAND ----------

SELECT
  firstName,
  lastName,
  salary,
  salary * 0.2 AS savings
FROM
  People10M

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Temporary Views
-- MAGIC So far, you've been working with Spark SQL by querying a table that we defined for you. In the following exercises, we will work with **temporary views**. Temporary views are useful for data exploration. It gives you a name to query from SQL, but unlike a table, does not carry over when you restart the cluster or switch to a new notebook. Also, temporary views will not show up in the `Data` tab. 
-- MAGIC 
-- MAGIC In the cell below, we create a temporary view that holds all the information from our last query, plus, adds another new column, `birthYear`. 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PeopleSavings AS
SELECT
  firstName,
  lastName,
  year(birthDate) as birthYear,
  salary,
  salary * 0.2 AS savings
FROM
  People10M;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Where are the results?!
-- MAGIC 
-- MAGIC When you create a temporary view, the "OK" at the bottom indicates that your command ran successfully, but the view itself does not automatically appear. To see the records in the the temporary view, you can run a query on it. 

-- COMMAND ----------

SELECT * FROM PeopleSavings;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Query Views
-- MAGIC For the most part, you can query a view exactly as you would query a table. The query below uses the built-in function `AVG()` to calculate `avgSalary` **grouped by** `birthYear`. This is an aggregate function, which means it's meant perform an calculation on a set of values. You must include a `GROUP BY` clause to identify the subset of values you want to summarize. 
-- MAGIC 
-- MAGIC The final clause, `ORDER BY`, declares the column that will control the order in which the rows appear, and the keyword `DESC` means they will appear in descending order. 
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We use a `ROUND()` function around the `AVG()` to round to the nearest cent. 

-- COMMAND ----------

SELECT
  birthYear,
  ROUND(AVG(salary), 2) AS avgSalary
FROM
  peopleSavings
GROUP BY
  birthYear
ORDER BY
  avgSalary DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Define a new table
-- MAGIC 
-- MAGIC Now we will show you how to create a table using Parquet. <a href="https://databricks.com/glossary/what-is-parquet#:~:text=Parquet%20is%20an%20open%20source,like%20CSV%20or%20TSV%20files" target="_blank">Parquet</a> is an open-source, column-based file format. Apache Spark supports many different file formats; you can specify how you want your table to be written with the `USING` keyword. 
-- MAGIC 
-- MAGIC 
-- MAGIC For now, we will focus on the commands we will use to create a new table. 
-- MAGIC 
-- MAGIC This data contains information about the relative popularity of first names in the United States by year from 1880 - 2016.
-- MAGIC 
-- MAGIC `Line 1`: Tables must have unique names. By including the `DROP TABLE IF EXISTS` command, we are ensuring that the next line (`CREATE TABLE`) can run successfully even if this table has already been created. The semi-colon at the end of the line allows us to run another command in the same cell. 
-- MAGIC 
-- MAGIC `Line 2`: Creates a table named `ssaNames`, defines the data source type (`parquet`) and indicated that there are some optional parameters to follow. 
-- MAGIC 
-- MAGIC `Line 3`: Identifies the path to the file in object storage
-- MAGIC 
-- MAGIC `Line 4`: Indicates that the first line of the table should be treated as a header. 

-- COMMAND ----------

DROP TABLE IF EXISTS ssaNames;
CREATE TABLE ssaNames USING parquet OPTIONS (
  path "/mnt/training/ssn/names.parquet",
  header "true"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preview the data
-- MAGIC Run the cell below to preview the data. Notice that the `LIMIT` keyword restricts the number of returned rows to the specified limit. 

-- COMMAND ----------

SELECT
  *
FROM
  ssaNames
LIMIT
  5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Joining two tables
-- MAGIC 
-- MAGIC We can combine these tables to get a sense of how the data may be related. For example, you may wonder
-- MAGIC > How many popular first names appear in our generated `People10M` dataset?
-- MAGIC 
-- MAGIC We will use a join to help answer this question. We will perform the join in a series of steps.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Count distinct values
-- MAGIC 
-- MAGIC First, we query tables to get a list of the distinct values in any field. Run the commands below to see the number of distinct names are in each of our tables.

-- COMMAND ----------

SELECT count(DISTINCT firstName)
FROM SSANames;

-- COMMAND ----------

SELECT count(DISTINCT firstName) 
FROM People10M;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create temporary views
-- MAGIC Next, we create two temporary views so that the actual join will be easy to read/write. 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW SSADistinctNames AS 
  SELECT DISTINCT firstName AS ssaFirstName 
  FROM SSANames;

CREATE OR REPLACE TEMPORARY VIEW PeopleDistinctNames AS 
  SELECT DISTINCT firstName 
  FROM People10M

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Perform join
-- MAGIC Now, we can use the view names to **join** the two data sets. If you are new to using SQL, you may want to learn more about the different types of joins you can perform.  This [wikipedia article](https://en.wikipedia.org/wiki/Join_(SQL) offers complete explanations, with pictures and sample SQL code.  
-- MAGIC 
-- MAGIC By default, the join type shown here is `INNER`. That means the results will contain the intersection of the two sets, and any names that are not in both sets will not appear. Note, becuase it is default, we did not specify the join type. 

-- COMMAND ----------

SELECT firstName 
FROM PeopleDistinctNames 
JOIN SSADistinctNames ON firstName = ssaFirstName

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How many names?
-- MAGIC 
-- MAGIC To answer the question posed previously, we can perform this join and include a count of the number of records in the result. 

-- COMMAND ----------

SELECT count(*) 
FROM PeopleDistinctNames 
JOIN SSADistinctNames ON firstName = ssaFirstName;

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
