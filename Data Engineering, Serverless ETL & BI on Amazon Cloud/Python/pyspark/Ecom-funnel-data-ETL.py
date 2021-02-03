from pyspark.context import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as f

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import sys

# args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME','file_name'])

file_name = args['file_name']

conf = SparkConf()

conf.set("spark.sql.parquet.compression.codec","snappy")
conf.set("spark.sql.parquet.writeLegacyFormat","true")

output_dir_path = "s3://bucket-name/parquet_output"

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)

input_file_path = "s3://bucket-name/user_behaviour/"+file_name

df = spark.read.option("header","true")\
	.option("inferSchema","true")\
	.option("quote","\"")\
	.option("escape","\"").csv(input_file_path)


df = df.withColumn('event_timestamp',f.to_timestamp('event_timestamp',format='MM/dd/yyyy HH:mm'))


df= df.withColumn('year',f.year(f.col('event_timestamp')))\
	.withColumn('month',f.month(f.col('event_timestamp')))


df.write.partitionBy(['year','month']).mode('append').format('parquet').save(output_dir_path)