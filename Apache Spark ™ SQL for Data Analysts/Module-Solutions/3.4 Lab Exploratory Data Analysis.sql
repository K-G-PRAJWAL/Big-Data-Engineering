-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lab 1 - Exploratory Data Analysis
-- MAGIC ## Module 3 Assignment
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this assignment you will: </br>
-- MAGIC 
-- MAGIC * Create tables
-- MAGIC * Create temporary views
-- MAGIC * Write basic SQL queries to explore, manipulate, and present data
-- MAGIC * Join two views and visualize the result
-- MAGIC 
-- MAGIC As you work through these exercises, you will be prompted to enter selected answers in Coursera. Find the quiz associated with this lab to enter your answers. 
-- MAGIC 
-- MAGIC Run the cell below to prepare this workspace for the lab. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Working with Retail Data
-- MAGIC For this assignment,
-- MAGIC we'll be working with a generated dataset meant to mimic data collected for online retail transactions. It was added to your workspace when you ran the previous cell. You can use the following path to access the data: 
-- MAGIC `/mnt/training/online_retail/data-001/data.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 1:  Create a table
-- MAGIC Summary: Create a new table named `outdoorProducts` with the following schema: 
-- MAGIC 
-- MAGIC 
-- MAGIC |Column Name | Type |
-- MAGIC | ---------- | ---- |
-- MAGIC |invoiceNo   | STRING |
-- MAGIC |stockCode   | STRING |
-- MAGIC |description | STRING |
-- MAGIC |quantity    | INT |
-- MAGIC |invoiceDate | STRING|
-- MAGIC |unitPrice  | DOUBLE | 
-- MAGIC |customerID  | INT |
-- MAGIC |countryName | STRING|
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Make sure this notebook is idempotent by dropping any tables that have the name `outdoorProducts`
-- MAGIC * Use `csv` as the specified data source
-- MAGIC * Use the path provided above to access the data
-- MAGIC * This data contains a header; include that in your table creation statement

-- COMMAND ----------

-- ANSWER
DROP TABLE IF EXISTS outdoorProducts;
CREATE TABLE outdoorProducts (
  invoiceNo STRING,
  stockCode STRING,
  description STRING,
  quantity INT,
  invoiceDate STRING,
  unitPrice DOUBLE,
  customerID STRING,
  countryName STRING
) USING csv OPTIONS (
  path "/mnt/training/online_retail/data-001/data.csv",
  header "true"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercise 2: Explore the data
-- MAGIC 
-- MAGIC **Summary:** Count the number of items that have a negative `quantity`
-- MAGIC 
-- MAGIC This table keeps track of online transactions, including returns. Some of the quantities in the `quantity` column show a negative number. Run a query that counts then number of negative values in the `quantity` column. 
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Write a query that reports the number of values less than 0 in the `quantity` column
-- MAGIC * **Report the answer in the corresponding quiz in Coursera**

-- COMMAND ----------

-- ANSWER
SELECT
  count(*)
FROM
  outdoorProducts
WHERE
  quantity < 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 3: Create a temporary view
-- MAGIC 
-- MAGIC **Summary:** Create a temporary view that includes only the specified columns and rows, and uses math to create a new column. 
-- MAGIC 
-- MAGIC **Steps to complete:**
-- MAGIC * Create a temporary view named `sales`
-- MAGIC * Create a new column, `totalAmount`, by multiplying `quantity` times `unitPrice` and rounding to the nearest cent
-- MAGIC * Include columns: `stockCode`, `quantity`, `unitPrice`, `totalAmount`, `countryName`
-- MAGIC * Include only rows where `quantity` is greater than 0

-- COMMAND ----------

-- ANSWER
CREATE
OR REPLACE TEMPORARY VIEW sales AS
SELECT
  stockCode,
  quantity,
  unitPrice,
  ROUND(quantity * unitPrice, 2) AS totalAmount,
  countryName
FROM
  outdoorProducts
WHERE
 quantity > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 4: Display ordered view
-- MAGIC 
-- MAGIC **Summary:** Show the view you created with `totalAmount` sorted greatest to least
-- MAGIC 
-- MAGIC **Steps to complete: **
-- MAGIC * Select all columns form the view `sales`
-- MAGIC * Order the `totalAmount` column from greatest to least
-- MAGIC * **Report the `countryName` from the row with the greatest `totalAmount` of sales in the corresponding answer area in Coursera**

-- COMMAND ----------

-- ANSWER
SELECT
  *
FROM
  sales
ORDER BY
  totalAmount DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 5: View countries
-- MAGIC **Summary:** Show a list of all unique `countryName` values in the `sales` view
-- MAGIC 
-- MAGIC **Steps to complete:**
-- MAGIC * Write a query that returns only distinct `countryName` values
-- MAGIC * **Answer the corresponding question in Coursera**

-- COMMAND ----------

-- ANSWER
SELECT
  DISTINCT(countryName)
FROM
  sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 6: Create a temporary view: `salesQuants`
-- MAGIC **Summary:** Create a temporary view that shows total `quantity` of items purchased from each `countryName`
-- MAGIC 
-- MAGIC **Steps to complete:** 
-- MAGIC * Create a temporary view named `salesQuants`
-- MAGIC * Display the sum of all `quantity` values grouped by `countryName`. Name that column `totalQuantity`
-- MAGIC * Order the view by `totalQuantity` from greatest to least
-- MAGIC * **Answer the corresponding question in Coursera** 

-- COMMAND ----------

-- ANSWER
CREATE
OR REPLACE TEMP VIEW salesQuants AS
SELECT
  SUM(quantity) AS totalQuantity,
  countryName
FROM
  sales
GROUP BY
  countryName
ORDER BY
  totalQuantity DESC;

SELECT * FROM salesQuants;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 7: Read in a new parquet table
-- MAGIC **Summary:** Create a new table named `countryCodes`.
-- MAGIC 
-- MAGIC **Steps to complete:** 
-- MAGIC * Drop any existing tables named `countryCodes` from your database
-- MAGIC * Use this path: `/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet` to create a new table using parquet as the data source. Name it `countryCodes`
-- MAGIC * Include options to indicate that there **is** a header for this table

-- COMMAND ----------

-- ANSWER

DROP TABLE IF EXISTS countryCodes;
CREATE TABLE countryCodes USING parquet OPTIONS (
  path "/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet",
  header "true"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 8: View metadata
-- MAGIC **Summary:** View column names and data types in this table.
-- MAGIC 
-- MAGIC **Steps to complete:**
-- MAGIC * Use the `DESCRIBE` command to display all of column names and their data types
-- MAGIC * **Answer the corresponding question in Coursera** 

-- COMMAND ----------

-- ANSWER 
DESCRIBE countryCodes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 9: Join and Visualize
-- MAGIC 
-- MAGIC **Summary:** Use the `salesQuants` view and the `countryCodes` table to display a pie chart that shows total sales by country, and identifies the country by its 3-letter id. 
-- MAGIC 
-- MAGIC **Steps to complete:** 
-- MAGIC * Write a query that results in two columns: `totalQuantity` from `salesQuants` and `alpha3Code` from `countryCodes`
-- MAGIC * Join `countryCodes` with `salesQuants` on the name of country listed in each table
-- MAGIC * Visualize your results as a pie chart that shows the percent of sales from each country

-- COMMAND ----------

-- ANSWER
SELECT
  totalQuantity,
  countryCodes.alpha3Code AS countryAbbr
FROM
  salesQuants
  JOIN countryCodes ON countryCodes.EnglishShortName = salesQuants.CountryName

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sanity Check
-- MAGIC It's always smart to do a sanity check when manipulating and joining datasets.  
-- MAGIC * Compare your chart to the table you displayed in task #4
-- MAGIC * Try the challenge problem to figure out what may have gone wrong

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Challenge: Find the problem
-- MAGIC 
-- MAGIC <script type="text/javascript">
-- MAGIC   window.onload = function() {
-- MAGIC     var allHints = document.getElementsByClassName("hint-9672");
-- MAGIC     var answer = document.getElementById("answer-9672");
-- MAGIC     var totalHints = allHints.length;
-- MAGIC     var nextHint = 0;
-- MAGIC     var hasAnswer = (answer != null);
-- MAGIC     var items = new Array();
-- MAGIC     var answerLabel = "Click here for the answer";
-- MAGIC     for (var i = 0; i < totalHints; i++) {
-- MAGIC       var elem = allHints[i];
-- MAGIC       var label = "";
-- MAGIC       if ((i + 1) == totalHints)
-- MAGIC         label = answerLabel;
-- MAGIC       else
-- MAGIC         label = "Click here for the next hint";
-- MAGIC       items.push({label: label, elem: elem});
-- MAGIC     }
-- MAGIC     if (hasAnswer) {
-- MAGIC       items.push({label: '', elem: answer});
-- MAGIC     }
-- MAGIC 
-- MAGIC     var button = document.getElementById("hint-button-9672");
-- MAGIC     if (totalHints == 0) {
-- MAGIC       button.innerHTML = answerLabel;
-- MAGIC     }
-- MAGIC     button.onclick = function() {
-- MAGIC       items[nextHint].elem.style.display = 'block';
-- MAGIC       if ((nextHint + 1) >= items.length)
-- MAGIC         button.style.display = 'none';
-- MAGIC       else
-- MAGIC         button.innerHTML = items[nextHint].label;
-- MAGIC         nextHint += 1;
-- MAGIC     };
-- MAGIC     button.ondblclick = function(e) {
-- MAGIC       e.stopPropagation();
-- MAGIC     }
-- MAGIC     var answerCodeBlocks = document.getElementsByTagName("code");
-- MAGIC     for (var i = 0; i < answerCodeBlocks.length; i++) {
-- MAGIC       var elem = answerCodeBlocks[i];
-- MAGIC       var parent = elem.parentNode;
-- MAGIC       if (parent.name != "pre") {
-- MAGIC         var newNode = document.createElement("pre");
-- MAGIC         newNode.append(elem.cloneNode(true));
-- MAGIC         elem.replaceWith(newNode);
-- MAGIC         elem = newNode;
-- MAGIC       }
-- MAGIC       elem.ondblclick = function(e) {
-- MAGIC         e.stopPropagation();
-- MAGIC       };
-- MAGIC 
-- MAGIC       elem.style.marginTop = "1em";
-- MAGIC     }
-- MAGIC   };
-- MAGIC </script>
-- MAGIC 
-- MAGIC <div>
-- MAGIC   <button type="button" class="btn btn-light"
-- MAGIC           style="margin-top: 1em"
-- MAGIC           id="hint-button-9672">Click here for a hint</button>
-- MAGIC </div>
-- MAGIC <div class="hint-9672" style="padding-bottom: 20px; display: none">
-- MAGIC   Hint:
-- MAGIC   <div style="margin-left: 1em">Display a distinct list of all countries in the countryCodes table. Compare that list with the distinct list of countries from the outdoorProducts chart.</div>
-- MAGIC </div>
-- MAGIC 
-- MAGIC 
-- MAGIC <div class="hint-9672" style="padding-bottom: 20px; display: none">
-- MAGIC   Hint:
-- MAGIC   <div style="margin-left: 1em">Use the `REPLACE()` command to make `United Kingdom` labels identical in for both datasets</div>
-- MAGIC </div>
-- MAGIC 
-- MAGIC 
-- MAGIC <div class="hint-9672" style="padding-bottom: 20px; display: none">
-- MAGIC   Hint:
-- MAGIC   <div style="margin-left: 1em">Rejoin and visualize the two datasets to show which countries sales are coming from</div>
-- MAGIC </div>
-- MAGIC 
-- MAGIC 
-- MAGIC <div id="answer-9672" style="padding-bottom: 20px; display: none">
-- MAGIC   The answer:
-- MAGIC   <div class="answer" style="margin-left: 1em">
-- MAGIC Check the solution set at the end of this module for one possible answer.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- ANSWER
SELECT DISTINCT(EnglishShortName) FROM countryCodes ORDER BY EnglishShortName DESC;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW modCountryCodes AS
SELECT
  alpha3code,
  REPLACE (
    EnglishShortName,
    "United Kingdom of Great Britain and Northern Ireland",
    "United Kingdom"
  ) AS EnglishShortName
FROM
  countryCodes;

-- COMMAND ----------

-- ANSWER
SELECT
  totalQuantity,
  modCountryCodes.alpha3Code AS countryAbbr
FROM
  salesQuants
  JOIN modCountryCodes ON modCountryCodes.EnglishShortName = salesQuants.CountryName

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
