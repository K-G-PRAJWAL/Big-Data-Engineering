-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Course Overview
-- MAGIC 
-- MAGIC Welcome! These notebooks are meant to help you prepare for the Databricks SQL Analyst accreditation (_coming soon_). The accredidation is meant to assess your understanding of basic Spark Architecture as well as your ability to write SQL queries with Spark SQL. In these notebooks, you will find practical challenges (and their answers) that will help you prepare for the skills-based assessment portion of the accreditation. 
-- MAGIC 
-- MAGIC You can learn more about how the accreditation exam will be structured in this [Exam Outline]($./Exam Outline). 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Navigating this course
-- MAGIC 
-- MAGIC There are 20 coding challenges, divided by topic, across seven notebooks. You can navigate to them using the links below, or by navigating to the Challenges folder in this file. 
-- MAGIC 
-- MAGIC Each challenge is designed to work with custom generated data. In each notebook, you **must** run the first cell to generate data for the challenges in that notebook. 
-- MAGIC 
-- MAGIC Each notebook contains questions related to the notebook topic. Your primary goal is outlined in the `Summary` within the question cell and specifics about how to answer are detailed under `Steps to Complete`. 
-- MAGIC 
-- MAGIC Each question is followed by a blank `-- TODO` cell.  Write your answer query in that cell. 
-- MAGIC 
-- MAGIC ### Links to practice notebooks
-- MAGIC 
-- MAGIC 1. [Basic Queries]($./Challenges/01 - Basic Queries)
-- MAGIC 1. [Manage Query Results]($./Challenges/02 - Manage Query Results)
-- MAGIC 1. [Joins]($./Challenges/03 - Joins)
-- MAGIC 1. [Timestamp Functions]($./Challenges/04 - Timestamp Functions)
-- MAGIC 1. [Aggregate Functions]($./Challenges/05 - Aggregate Functions)
-- MAGIC 1. [Nested Data]($./Challenges/06 - Nested Data)
-- MAGIC 1. [Higher Order Functions]($./Challenges/07 - Higher Order Functions)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## Check your work
-- MAGIC This file also includes a Solutions folder, where you can find answers to all challenges. 
-- MAGIC 
-- MAGIC **Note:** Some questions ask you to create a new temporary view and some simply ask you to display the results of a query.  For questions where you have created a new temporary view, we have also included a `SELECT *` statement show the resulting view in the Solutions notebook. These statements are written for demonstration purposes and are not part of the correct answer query. 
-- MAGIC 
-- MAGIC **Note:** There are many ways you could write a SQL query that successfully achieves all listed requirements. The given solution is not the only correct solution; it is offered as an example solution.  

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
