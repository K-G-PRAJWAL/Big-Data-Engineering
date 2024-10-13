- Dataset:

  - http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
  - Get the dataset and unzip

  ```
  wget -O ~/environment/scripts.zip https://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee.s3.us-east-1.amazonaws.com/795e88bb-17e2-498f-82d1-2104f4824168/scripts.zip
  unzip -o ~/environment/scripts.zip -d ~/environment
  ```

- Infra setup roles:

```
// S3_BUCKET_NAME
aws s3 ls  | grep -i airflow | awk '{print $3}'

// GLUE_ROLE_ARN
aws iam get-role --role-name AWSGlueServiceRoleDefault --query 'Role.[Arn]' --output text

// EXEC_ROLE_ARN
aws iam get-role --role-name MwaaExecutionRole --query 'Role.[Arn]' --output text
```

- Redshift setup:

  - Connection: `redshift-conn-id`

  - Attributes:

```
{
  "iam": true,
  "is_serverless": true,
  "serverless_work_group": "xxxxxxxx",
  "port": 5439,
  "db_user": "admin"
}
```

```
--    Create nyc schema.
CREATE schema nyc;

--    Create agg_green_rides table.
CREATE TABLE IF not EXISTS nyc.green (
  pulocationid      bigint,
  trip_type         bigint,
  payment_type        bigint,
  total_fare_amount    float
);

create user "IAMR:EXEC_ROLE" password DISABLE;

grant usage on SCHEMA nyc to "IAMR:EXEC_ROLE";

grant all on TABLE nyc.green to "IAMR:EXEC_ROLE";
```

- Deferrable Operators:
  - Standard operators and sensors continuously occupy an Airflow worker slot, regardless of whether they are active or idle.
  - With the introduction of deferrable operators in Apache Airflow 2.2, the polling process can be offloaded to ensure efficient utilization of the worker slot.
  - A deferrable operator can suspend itself and resume once the external job is complete, instead of continuously occupying a worker slot.
