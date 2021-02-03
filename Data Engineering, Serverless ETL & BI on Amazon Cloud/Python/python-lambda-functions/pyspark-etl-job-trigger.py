import boto3 

def lambda_handler(event, context):

    
    client = boto3.client("glue")

    file_key = event['Records'][0]['s3']['object']['key'].split("/")[1]
        
    client.start_job_run(
        JobName = 'Funnel_data_etl_pyspark',
        Arguments = {
        '--file_name':file_key
        }
    )
    return {
        'statusCode': 200,
        'body': json.dumps('Funnel_data_etl_pyspark triggered')
    }
    