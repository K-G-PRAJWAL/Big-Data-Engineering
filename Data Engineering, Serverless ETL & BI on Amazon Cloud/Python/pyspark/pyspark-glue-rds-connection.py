from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext,SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3 ,json,sys


secret_name = 'your-rds-secret-name'
region_name ='eu-west-1'
session = boto3.session.Session()
client = session.client(service_name='secretsmanager',region_name=region_name)
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
creds = json.loads(get_secret_value_response['SecretString'])

username = creds['username']
password = creds['password']
host = creds['host']

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
partition_by_cols=["year","month"]
output_dir_path="s3://bucket-name/orders_data_pyspark"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


conf = SparkConf()
conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
conf.set("spark.sql.parquet.compression.codec","snappy")
conf.set("spark.sql.parquet.writeLegacyFormat","true")

jdbc_url = "jdbc:mysql://"+host+":3306/ecommerce_db"

sql_qry = """
            (
				select * , 
				year(order_purchase_timestamp) as year ,
				month(order_purchase_timestamp) as month
				 from orders
            ) as t

          """

spark_df = glueContext.read.format("jdbc").option("url",jdbc_url) \
            .option("user",username) \
            .option("password",password) \
            .option("dbtable",sql_qry).option("driver","com.mysql.jdbc.Driver").load()

spark_df.write.partitionBy(partition_by_cols).mode("append").format("parquet").save(output_dir_path)