from kafka import KafkaProducer
from faker import Faker
import json
from time import sleep
import uuid

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Using faker to generate dummy data
_instance = Faker()
global faker
faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data():
        return [
            uuid.uuid4().__str__(),
            faker.name(),
            faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
            faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
            faker.random_int(min=10000, max=150000),
            faker.random_int(min=18, max=60),
            faker.random_int(min=0, max=100000),
            faker.unix_time()
        ]

# Generate random data at 5 secs interval
for _ in range(20):
    for i in range(1, 20):
        columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
        data_list = DataGenerator.get_data()
        json_data = dict(zip(columns, data_list))
        payload = json.dumps(json_data).encode("utf-8")
        response = producer.send('FirstTopic', payload)
        print(payload)
        sleep(5)
