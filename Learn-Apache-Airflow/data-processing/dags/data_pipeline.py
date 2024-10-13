from os import path

from datetime import timedelta

import airflow
from airflow import DAG

from airflow.providers.amazon.aws.operators.emr import EmrServerlessCreateApplicationOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessDeleteApplicationOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# CONSTANTS
S3_BUCKET_NAME = "airflow-xxx-bucket"
GLUE_ROLE_ARN = "arn:aws:iam::669271927xxx:role/AWSGlueServiceRoleDefault"
EXEC_ROLE_ARN = "arn:aws:iam::669271927xxx:role/service-role/MwaaExecutionRole"

dag_name = 'data_pipeline'

correlation_id = "{{ run_id }}"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# DAG definition
dag = DAG(
    dag_name,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule='0 3 * * *'
)

# S3 Sensor - add a task in the DAG that waits for objects to be available in S3 before executing next set of tasks
s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    bucket_name=S3_BUCKET_NAME,
    bucket_key='data/raw/green*',
    wildcard_match=True,
    dag=dag
)

# Glue Crawler - run the Glue crawler through an Operator in Airflow
glue_crawler_config = {
    "Name": "airflow-workshop-raw-green-crawler",
    "Role": GLUE_ROLE_ARN,
    "DatabaseName": "default",
    "Targets": {"S3Targets": [{"Path": f"{S3_BUCKET_NAME}/data/raw/green"}]},
}

glue_crawler = GlueCrawlerOperator(
    task_id="glue_crawler",
    config=glue_crawler_config,
    dag=dag
)

# Glue ETL Job
glue_job = GlueJobOperator(
    task_id="glue_job",
    job_name="nyc_raw_to_transform",
    script_location=f"s3://{S3_BUCKET_NAME}/scripts/glue/nyc_raw_to_transform.py",
    s3_bucket=S3_BUCKET_NAME,
    iam_role_name="AWSGlueServiceRoleDefault",
    create_job_kwargs={"GlueVersion": "4.0",
                       "NumberOfWorkers": 2, "WorkerType": "G.1X"},
    script_args={'--dag_name': dag_name,
                 '--task_id': 'glue_task',
                 '--correlation_id': correlation_id},
    dag=dag
)

# Create Serverless EMR
create_emr_serverless_app = EmrServerlessCreateApplicationOperator(
    task_id="create_emr_serverless_app",
    aws_conn_id="aws_default",
    release_label="emr-6.9.0",
    job_type="SPARK",
    config={"name": "my-emr-serverless-app"},
    dag=dag
)

# Submit EMR job
emr_serverless_job = EmrServerlessStartJobOperator(
    task_id="start_emr_serverless_job",
    aws_conn_id="aws_default",
    application_id="{{ task_instance.xcom_pull('create_emr_serverless_app', key='return_value') }}",
    execution_role_arn=EXEC_ROLE_ARN,
    job_driver={
        "sparkSubmit": {
            "entryPoint": f"s3://{S3_BUCKET_NAME}/scripts/emr/nyc_aggregations.py",
            "entryPointArguments": [f"s3://{S3_BUCKET_NAME}/data/transformed/green",
                                    f"s3://{S3_BUCKET_NAME}/data/aggregated/green",
                                    dag_name,
                                    'start_emr_serverless_job',
                                    correlation_id],
            "sparkSubmitParameters": "--conf spark.executor.instances=2 --conf spark.executor.memory=4G --conf spark.executor.cores=2 --conf spark.executor.memoryOverhead=1G"

        }
    },
    configuration_overrides={
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": f"s3://{S3_BUCKET_NAME}/logs/emr/data-pipeline/create_emr_cluster/"
            }
        }
    },
    dag=dag
)

# Sensor to wait for EMR job to complete
emr_serverless_job_sensor = EmrServerlessJobSensor(
    task_id="wait_for_emr_serverless_job",
    aws_conn_id="aws_default",
    application_id="{{ task_instance.xcom_pull('create_emr_serverless_app', key='return_value') }}",
    job_run_id="{{ task_instance.xcom_pull('start_emr_serverless_job', key='return_value') }}",
    dag=dag
)

# EMR termination
delete_app = EmrServerlessDeleteApplicationOperator(
    task_id="delete_app",
    application_id="{{ task_instance.xcom_pull('create_emr_serverless_app', key='return_value') }}",
    dag=dag
)

# Copy transformed S3 data to redshift(serverless)
copy_to_redshift = S3ToRedshiftOperator(
    task_id='copy_to_redshift',
    schema='nyc',
    table='green',
    s3_bucket=S3_BUCKET_NAME,
    s3_key='data/aggregated',
    copy_options=["FORMAT AS PARQUET"],
    redshift_conn_id='redshift-conn-id',
    dag=dag
)

# Workflow
s3_sensor >> glue_crawler >> glue_job >> create_emr_serverless_app >> emr_serverless_job >> emr_serverless_job_sensor >> delete_app >> copy_to_redshift
