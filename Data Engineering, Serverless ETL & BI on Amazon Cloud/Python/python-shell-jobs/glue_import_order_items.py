import boto3,json 
from pg import DB 

secret_name = 'your-secret-name'
region_name ='us-east-1'

session = boto3.session.Session()

client = session.client(service_name='secretsmanager',region_name=region_name)

get_secret_value_response = client.get_secret_value(SecretId=secret_name)

creds = json.loads(get_secret_value_response['SecretString'])

username = creds['username']
password = creds['password']
host = creds['host']

db = DB(dbname='dev',host=host,port=5439,user=username,passwd=password)

merge_qry = """
			begin ; 

			copy mysql_dwh_staging.order_items from 's3://bucket_name/order_items/current/order_items.csv'
			iam_role 'YOUR_ARN'
			CSV QUOTE '\"' DELIMITER ','
			acceptinvchars;

			delete 
				from 
					mysql_dwh.order_items 
				using mysql_dwh_staging.order_items 
				where mysql_dwh.order_items.order_item_id = mysql_dwh_staging.order_items.order_item_id ;

			insert into mysql_dwh.order_items select * from mysql_dwh_staging.order_items;

			truncate table mysql_dwh_staging.order_items;
			
			end ; 

			"""

result = db.query(merge_qry)
print(result)
