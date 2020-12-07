-- Databricks notebook source
-- MAGIC 
-- MAGIC %run ./Common

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # SETUP
-- MAGIC 
-- MAGIC def setup():
-- MAGIC   createDummyData("dcad_50_p", UTCTime="Timestamp").createOrReplaceTempView("timetable1")
-- MAGIC 
-- MAGIC setup()
-- MAGIC 
-- MAGIC displayHTML("""
-- MAGIC Declared the following table:
-- MAGIC   <li><span style="color:green; font-weight:bold">timetable1</span> </li>
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC 
-- MAGIC # SETUP
-- MAGIC 
-- MAGIC def setup():
-- MAGIC   return createDummyData("dcad_86_p", UTCTime="Timestamp").createOrReplaceTempView("timetable2")
-- MAGIC 
-- MAGIC df = setup()
-- MAGIC 
-- MAGIC displayHTML("""
-- MAGIC Declared the following table:
-- MAGIC    <li><span style="color:green; font-weight:bold">timetable2</span></li>
-- MAGIC """.format(df))
