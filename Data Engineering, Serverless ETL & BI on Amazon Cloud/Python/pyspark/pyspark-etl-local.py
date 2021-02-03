#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.context import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as f


# In[3]:


conf = SparkConf()


# In[4]:


conf.set("spark.sql.parquet.compression.codec","snappy")
conf.set("spark.sql.parquet.writeLegacyFormat","true")


# In[5]:


output_dir_path = "path_to_directory/ecom-funnel-data/output_result"


# In[6]:


sc = SparkContext()

spark = SQLContext(sc)


# In[7]:


input_file_path = "path_to_directory/ecom-funnel-data/2016_funnel.csv"


# In[9]:


df = spark.read.option("header","true")    .option("inferSchema","true")    .option("quote","\"")    .option("escape","\"").csv(input_file_path)


# In[11]:


df.printSchema()


# In[12]:


df = df.withColumn('event_timestamp',f.to_timestamp('event_timestamp',format='MM/dd/yyyy HH:mm'))


# In[16]:


df= df.withColumn('year',f.year(f.col('event_timestamp')))    .withColumn('month',f.month(f.col('event_timestamp')))


# In[19]:


# df.select(['year','month']).show()


# In[20]:


df.write.partitionBy(['year','month']).mode('append').format('parquet').save(output_dir_path)

