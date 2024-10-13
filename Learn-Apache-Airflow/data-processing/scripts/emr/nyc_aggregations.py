from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("""
        Usage: nyc_aggregations.py <s3_input_path> <s3_output_path> <dag_name> <task_id> <correlation_id>
        """, file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    dag_task_name = sys.argv[3] + "." + sys.argv[4]
    correlation_id = dag_task_name + " " + sys.argv[5]

    spark = SparkSession\
        .builder\
        .appName(correlation_id)\
        .getOrCreate()

    sc = spark.sparkContext

    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(dag_task_name)
    logger.info("Spark session started: " + correlation_id)

    df = spark.read.parquet(input_path)
    df.printSchema
    df_out = df.groupBy('pulocationid', 'trip_type', 'payment_type').agg(
        sum('fare_amount').alias('total_fare_amount'))

    df_out.write.mode('overwrite').parquet(output_path)

    logger.info("Stopping Spark session: " + correlation_id)
    spark.stop()
