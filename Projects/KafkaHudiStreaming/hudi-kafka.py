import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import constants


spark_version = constants.SPARK_VERSION
SUBMIT_ARGS = f'--packages ' \
              f'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.1,' \
              f'org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},' \
              f'org.apache.kafka:kafka-clients:2.8.1 ' \
              f'pyspark-shell'

# Download dependencies(jars)
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

# Hudi Configurations
hudi_options = {
    'hoodie.table.name': constants.TABLE_NAME,
    'hoodie.datasource.write.recordkey.field': constants.RECORDKEY,
    'hoodie.datasource.write.table.name': constants.TABLE_NAME,
    'hoodie.datasource.write.operation': constants.METHOD,
    'hoodie.datasource.write.precombine.field': constants.PRECOMBINE,
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

# Initializing Spark Session
print('Initializing Spark...')
spark = SparkSession.builder \
    .master("local") \
    .appName("kafka-example") \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('spark.sql.warehouse.dir', constants.PATH) \
    .getOrCreate()

print('Spark Initialized...')

# Reading the Kafka Stream
print('Reading Kafka Stream...')
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", constants.BOOTSTRAP) \
    .option("subscribe", constants.TOPIC) \
    .option("startingOffsets", "latest") \
    .option("includeHeaders", "true") \
    .option("failOnDataLoss", "false") \
    .load()


def process_batch_message(df, batch_id):
    my_df = df.selectExpr("CAST(value AS STRING) as json")
    schema = StructType(
        [
            StructField("emp_id", StringType(), True),
            StructField("employee_name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("state", StringType(), True),
            StructField("salary", StringType(), True),
            StructField("age", StringType(), True),
            StructField("bonus", StringType(), True),
            StructField("ts", StringType(), True),
        ]
    )
    clean_df = my_df.select(from_json(col("json").cast("string"), schema).alias("parsed_value")).select(
        "parsed_value.*")

    if clean_df.count() > 0:
        clean_df.write.format("hudi"). \
            options(**hudi_options). \
            mode("append"). \
            save(constants.PATH)
    print("batch_id : ", batch_id, clean_df.show(truncate=False))

print('Writing Kafka Stream...')

query = kafka_df.writeStream \
    .foreachBatch(process_batch_message) \
    .option("checkpointLocation", "path/to/HDFS/dir") \
    .trigger(processingTime="1 minutes") \
    .start().awaitTermination()
