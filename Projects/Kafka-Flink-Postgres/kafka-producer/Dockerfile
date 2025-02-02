FROM python:3.8-slim

COPY requirements.txt .

RUN set -ex; \
  	pip install --no-cache-dir -r requirements.txt

WORKDIR /
COPY wait.sh wait.sh

ADD producer.py .

# Command to wait for zookeeper and kafka to be up and running before starting the producer
CMD ./wait.sh -s -t 30 $ZOOKEEPER_SERVER -- ./wait.sh -s -t 30 $KAFKA_SERVER -- python -u producer.py