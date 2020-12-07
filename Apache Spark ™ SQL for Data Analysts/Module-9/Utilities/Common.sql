-- Databricks notebook source
-- MAGIC 
-- MAGIC %run ./DummyDataGenerator

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # These imports are OK to provide for students
-- MAGIC import pyspark
-- MAGIC from typing import Callable, Any, Iterable, List, Set, Tuple

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
-- MAGIC 
-- MAGIC // Get the user's name
-- MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
-- MAGIC val userhome = s"dbfs:/user/$username"
-- MAGIC 
-- MAGIC spark.conf.set("com.databricks.training.username", username)
-- MAGIC spark.conf.set("com.databricks.training.userhome", userhome)
-- MAGIC 
-- MAGIC displayHTML("")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark import SparkContext
-- MAGIC from pyspark.sql import (SparkSession, DataFrame)
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC username = spark.conf.get("com.databricks.training.username")
-- MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
-- MAGIC 
-- MAGIC # *******************************************
-- MAGIC #  GET QUERY STRING
-- MAGIC # *******************************************
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC def getQueryString(df: DataFrame) -> str:
-- MAGIC   # redirect sys.stdout to a buffer
-- MAGIC   import sys, io
-- MAGIC   stdout = sys.stdout
-- MAGIC   sys.stdout = io.StringIO()
-- MAGIC 
-- MAGIC   # call module
-- MAGIC   df.explain(extended=True)
-- MAGIC 
-- MAGIC   # get output and restore sys.stdout
-- MAGIC   output = sys.stdout.getvalue()
-- MAGIC   sys.stdout = stdout
-- MAGIC 
-- MAGIC   return output
-- MAGIC 
-- MAGIC None
