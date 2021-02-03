from pyspark.context import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as f

from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *

from awsglue.context import GlueContext
from awsglue.job import Job
import sys

args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

conf = SparkConf()

conf.set("spark.sql.parquet.compression.codec","snappy")
conf.set("spark.sql.parquet.writeLegacyFormat","true")

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)

input_file_path = "s3://bucket-name/user_behaviour/2016_funnel.csv"

df = spark.read.option("header","true")\
	.option("inferSchema","true")\
	.option("quote","\"")\
	.option("escape","\"").csv(input_file_path)


df = df.withColumn('event_timestamp',f.to_timestamp('event_timestamp',format='MM/dd/yyyy HH:mm'))


df= df.withColumn('year',f.year(f.col('event_timestamp')))\
	.withColumn('month',f.month(f.col('event_timestamp')))


dynamic_df = DynamicFrame.fromDF(df, glueContext, "dynamic_df")

mapped_df = ResolveChoice.apply(frame = dynamic_df, choice = "make_cols",transformation_ctx = "mapped_df")

datasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = mapped_df, catalog_connection = "your_connection_name", connection_options = {"dbtable": "external_data_schema.user_behaviour", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink")

job.commit()