import json
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.operators.python_operator import PythonVirtualenvOperator

from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTrainingOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator

from preprocess import preprocess

# CONSTANTS
S3_BUCKET_NAME = "airflow-xxx-bucket"
REGION_NAME = "us-east-1"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
}

# loading sagemaker train and transform config and writing to S3
s3 = S3Hook()
sagemaker_config = [{}, {}]
sagemaker_config_json = json.dumps(sagemaker_config)

if not s3.check_for_key(key='task_storage/sagemaker_config.json', bucket_name=S3_BUCKET_NAME):
    s3.load_string(
        string_data=sagemaker_config_json,
        key='task_storage/sagemaker_config.json',
        bucket_name=S3_BUCKET_NAME,
        replace=True
    )


def getTrainTransformConfig(**kwargs):
    import json
    import boto3
    import datetime
    import sagemaker

    from sagemaker.amazon.amazon_estimator import get_image_uri
    from sagemaker.estimator import Estimator
    from sagemaker.workflow.airflow import training_config, transform_config_from_estimator

    def serialize_datetime(obj):
        if isinstance(obj, datetime.datetime):
            return obj.ifofformat()
        raise TypeError("Unserializable type")

    # Setup Config for model
    bucket_name = kwargs['bucket_name']
    region_name = kwargs['region_name']
    config = {}

    config["job_level"] = {
        "region_name": region_name,
        "run_hyperparameter_opt": "no"
    }

    config["train_model"] = {
        "sagemaker_role": "AirflowSageMakerExecutionRole",
        "estimator_config": {
            "train_instance_count": 1,
            "train_instance_type": "ml.m5.xlarge",
            "train_volume_size": 5,   # %GB storage
            "train_max_run": 3600,
            "output_path": "s3://{}/xgboost/output".format(bucket_name),
            "hyperparameters": {
                "feature_dim": "178729",
                "epochs": "10",
                "mini_batch_size": "200",
                "num_factors": "64",
                "predictor_type": "regressor",
                "max_depth": "5",
                "eta": "0.2",
                "objective": "reg\:linear",
                "early_stopping_rounds": "10",
                "num_round": "150"
            }
        },
        "inputs": {
            "train": "s3://{}/xgboost/train/train.csv".format(bucket_name),
            "validation": "s3://{}/xgboost/validate/validate.csv".format(bucket_name)
        }
    }

    config["batch_transform"] = {
        "transform_config": {
            "instance_count": 1,
            "instance_type": "ml.m5.xlarge",
            "data": "s3://{}/xgboost/test/".format(bucket_name),
            "data_type": "S3Prefix",
            "content_type": "text/csv",
            "strategy": "MultiRecord",
            "split_type": "Line",
            "output_path": "s3://{}/transform/".format(bucket_name)
        }
    }

    region = config["job_level"]["region_name"]  # Get region

    # Set boto3 sessions
    boto3_session = boto3.session.Session(region_name=region)
    sagemaker_session = sagemaker.session.Session(boto_session=boto3_session)

    iam = boto3.client('iam', region_name=region)
    role = iam.get_role(RoleName=config["train_model"]["sagemaker_role"])[
        'Role']['Arn']

    # Retrieve docker image from ECR
    container = sagemaker.image_uris.retrieve(
        region=region_name, framework='xgboost', version='1.7-1')

    # Setup train and validation data
    train_input = config["train_model"]["inputs"]["train"]
    csv_train_input = sagemaker.inputs.TrainingInput(
        train_input, content_type='csv')

    validation_input = config["train_model"]["inputs"]["validation"]
    csv_validation_input = sagemaker.inputs.TrainingInput(
        validation_input, content_type='csv')

    training_inputs = {"train": csv_train_input,
                       "validation": csv_validation_input}
    output_path = config["train_model"]["estimator_config"]["output_path"]

    fm_estimator = Estimator(image_uri=container,
                             role=role,
                             instance_count=1,
                             instance_type='ml.m5.2xlarge',
                             volume_size=5,
                             output_path=output_path,
                             sagemaker_session=sagemaker_session
                             )

    fm_estimator.set_hyperparameters(max_depth=5,
                                     eta=0.2,
                                     objective='reg:linear',
                                     early_stopping_rounds=10,
                                     num_round=150)

    train_config = training_config(
        estimator=fm_estimator, inputs=training_inputs)

    transform_config = transform_config_from_estimator(
        estimator=fm_estimator,
        task_id="model_tuning" if False else "model_training",
        task_type="tuning" if False else "training",
        **config["batch_transform"]["transform_config"]
    )

    # Write config to S3
    s3 = boto3.resource('s3')
    s3object = s3.Object(bucket_name, 'task_storage/sagemaker_config.json')
    s3object.put(
        Body=(bytes(json.dumps(
            [train_config, transform_config], default=serialize_datetime).encode('UTF-8')))
    )


# Airflow Dags
dag = DAG(
    'feature_engineering',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule=None
)


s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    bucket_name=S3_BUCKET_NAME,
    bucket_key='raw/ml_train_data.csv',
    dag=dag
)

# preprocess function
preprocess_task = PythonVirtualenvOperator(
    task_id='preprocessing_data',
    python_callable=preprocess,
    op_kwargs={'bucket_name': S3_BUCKET_NAME},
    requirements=["boto3", "pandas", "numpy", "fsspec", "s3fs"],
    use_dill=True,  # Use dill to serialize the Python callable
    # Do not include system site packages in the virtual environment
    system_site_packages=False,
    dag=dag
)

# Generate and fetch train and transform config
get_traintransform_config_task = PythonVirtualenvOperator(
    task_id='generate_config',
    python_callable=getTrainTransformConfig,
    op_kwargs={'bucket_name': S3_BUCKET_NAME, 'region_name': REGION_NAME},
    requirements=["boto3", "sagemaker"],
    use_dill=True,  # Use dill to serialize the Python callable
    # Do not include system site packages in the virtual environment
    system_site_packages=False,
    dag=dag
)

# launch sagemaker training job and wait until it completes
train_model_task = SageMakerTrainingOperator(
    task_id='model_training',
    dag=dag,
    config=json.loads(s3.read_key(bucket_name=S3_BUCKET_NAME,
                      key='task_storage/sagemaker_config.json'))[0],
    aws_conn_id='airflow-sagemaker',
    wait_for_completion=True,
    check_interval=30
)

# launch sagemaker batch transform job and wait until it completes
batch_transform_task = SageMakerTransformOperator(
    task_id='predicting',
    dag=dag,
    config=json.loads(s3.read_key(bucket_name=S3_BUCKET_NAME,
                      key='task_storage/sagemaker_config.json'))[1],
    aws_conn_id='airflow-sagemaker',
    wait_for_completion=True,
    check_interval=30,
    trigger_rule=TriggerRule.ONE_SUCCESS
)

# set the dependencies between tasks
s3_sensor >> preprocess_task >> get_traintransform_config_task >> train_model_task >> batch_transform_task
