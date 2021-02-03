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

			copy mysql_dwh_staging.order_reviews from 's3://bucket_name/order_reviews/current/order_reviews.csv'
			iam_role 'YOUR_ARN'
			CSV QUOTE '\"' DELIMITER ','
			acceptinvchars;

			delete 
				from 
					mysql_dwh.order_reviews 
				using mysql_dwh_staging.order_reviews 
				where mysql_dwh.order_reviews.review_id = mysql_dwh_staging.order_reviews.review_id;

			insert into mysql_dwh.order_reviews select * from mysql_dwh_staging.order_reviews;

			truncate table mysql_dwh_staging.order_reviews;
			
			end ; 

			"""

result = db.query(merge_qry)
print(result)
