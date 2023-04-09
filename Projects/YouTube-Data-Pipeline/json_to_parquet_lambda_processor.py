import pandas as pd
import os

import awswrangler as wr
import urllib.parse

os_input_s3_refined_layer = os.environ['s3_refined_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))
        # Only extract the "items" JSON key
        df_items = pd.json_normalize(df_raw['items'])

        wr_response = wr.s3.to_parquet(
            df=df_items,
            path=os_input_s3_refined_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )
        return wr_response

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}.'.format(key, bucket))
        raise
