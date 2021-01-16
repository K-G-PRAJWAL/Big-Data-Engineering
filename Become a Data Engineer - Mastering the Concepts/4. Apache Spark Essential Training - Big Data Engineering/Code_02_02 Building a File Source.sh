#This is an example script for setting up a Kafka Connect
#Task that uses a file system source to publish a topic.

#Command to run kafka-connect with a file source connector and a task.
/usr/lib/kafka/bin/connect-standalone.sh \
/usr/lib/kafka/config/connect-standalone.properties \
/home/cloudera/spark-data-engg/connect-file-source.properties

#Command to subscribe and listen
/usr/lib/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic file-source-test \
--from-beginning
#--------------------------------------------------
#Content of connect-file-source.properties
#--------------------------------------------------

#A logical name for your task
name=demo-file-source

#Class of connector to use
connector.class=FileStreamSource

#Number of parallel tasks to launch. Provides scalability
tasks.max=1

#Local file to monitor for new input
file=/home/cloudera/spark-data-engg/source-file/file-test.txt

#Topic to publish data to.
topic=file-source-test
