# Spark Configs
SPARK_VERSION='3.3.2'
SUBMIT_ARGS = f'--packages ' \
              f'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.1,' \
              f'org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION},' \
              f'org.apache.kafka:kafka-clients:2.8.1 ' \
              f'pyspark-shell'

# Hudi Configs
DB_NAME = "hudidb"
TABLE_NAME = "huditable"
RECORDKEY = 'emp_id'
PRECOMBINE = 'ts'
PATH = f"file:///E:/DATA/Projects/Big-Data-Engineering/Projects/KafkaHudiStreaming/warehouse/{DB_NAME}/{TABLE_NAME}"
METHOD = 'upsert'
TABLE_TYPE = "COPY_ON_WRITE"

# Kafka Configs
BOOTSTRAP = "localhost:9092"
TOPIC = "FirstTopic"
