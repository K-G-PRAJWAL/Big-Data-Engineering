import time
import random
import schedule

from json import dumps

from faker import Faker
from kafka import KafkaProducer


kafka_nodes = "kafka:9092"
topic = "weather"


def gen_data():
    faker = Faker()

    producer = KafkaProducer(bootstrap_servers=kafka_nodes,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    data = {'city': faker.city(), 'temperature': random.uniform(10.0, 110.0)}

    print(data)
    producer.send(topic=topic, value=data)

    producer.flush()


if __name__ == "__main__":
    gen_data()
    schedule.every(10).seconds.do(gen_data)

    while True:
        schedule.run_pending()
        time.sleep(0.5)
