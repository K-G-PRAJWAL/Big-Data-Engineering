import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Constant
S3_BUCKET_NAME = "airflow-xxx-bucket"

args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'dag_name', 'task_id', 'correlation_id'])

s3_bucket = S3_BUCKET_NAME

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
correlation_id = args['dag_name'] + "." + \
    args['task_id'] + " " + args['correlation_id']
logger.info("Correlation ID from GLUE job: " + correlation_id)

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="default", table_name="green", transformation_ctx="datasource0")
logger.info("After create_dynamic_frame.from_catalog: " + correlation_id)

applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=[("vendorid", "long", "vendorid", "long"), ("lpep_pickup_datetime", "string", "lpep_pickup_datetime", "string"), ("lpep_dropoff_datetime", "string", "lpep_dropoff_datetime", "string"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("ratecodeid", "long", "ratecodeid", "long"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), (
    "fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("ehail_fee", "string", "ehail_fee", "string"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double"), ("payment_type", "long", "payment_type", "long"), ("trip_type", "long", "trip_type", "long")], transformation_ctx="applymapping1")
logger.info("After ApplyMapping: " + correlation_id)

resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1, choice="make_struct", transformation_ctx="resolvechoice2")
logger.info("After ResolveChoice: " + correlation_id)

dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2, transformation_ctx="dropnullfields3")
logger.info("After DropNullFields: " + correlation_id)

datasink4 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields3, connection_type="s3", connection_options={
                                                         "path": f"s3://{s3_bucket}/data/transformed/green"}, format="parquet", transformation_ctx="datasink4")
logger.info("After write_dynamic_frame.from_options: " + correlation_id)

job.commit()
