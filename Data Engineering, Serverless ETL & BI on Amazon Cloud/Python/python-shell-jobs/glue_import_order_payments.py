import boto3,json 
from pg import DB 

secret_name = 'secret-name'
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
			copy mysql_dwh_staging.order_payment from 's3://bucket_name/order_payments/current/order_payments.csv'
			iam_role 'YOUR_ARN'
			CSV QUOTE '\"' DELIMITER ','
			acceptinvchars;
			delete 
				from 
					mysql_dwh.order_payments 
				using mysql_dwh_staging.order_payments 
				where mysql_dwh.order_payments.order_id = mysql_dwh_staging.order_payments.order_id ;
			insert into mysql_dwh.order_payments select * from mysql_dwh_staging.order_payments;
			truncate table mysql_dwh_staging.order_payments;
			end ; 
			"""

result = db.query(merge_qry)
print(result)
