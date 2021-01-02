#This is an example script for setting up a Kafka Connect
#Task that uses a File Source to publish topics.



#Command to run kafka-connect with a file source connector and a task.
/usr/lib/kafka/bin/connect-standalone.sh \
/usr/lib/kafka/config/connect-standalone.properties \
/home/cloudera/spark-data-engg/webclicks-file-source.properties

#Command to test topic
/usr/lib/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--zookeeper localhost:2181 \
--topic use-case-webclicks \
--from-beginning

#--------------------------------------------------
#Content of webclicks-file-source.properties
#--------------------------------------------------

#A logical name for your task
name=usecase-webclicks-source

#Class of connector to use
connector.class=FileStreamSource

#Number of parallel tasks to launch. Provides scalability
tasks.max=1

#Local file to monitor for new input
file=/home/cloudera/spark-data-engg/source-file/webclicks.txt

#Topic to publish data to.
topic=use-case-webclicks


#------------------------------------------------------------------
#This is the sample contents of webclicks.txt. These are click events
#---------------------------------------------------------------------
{ "timestamp": 1496767720000, "hitType": "event", "eventCategory": "firstPage", "eventAction": "enter", "eventLabel": "Fall Campaign"}
{ "timestamp": 1496767730000, "hitType": "event", "eventCategory": "browseProduct", "eventAction": "List", "eventLabel": "Mens wear"}
{ "timestamp": 1496767730000, "hitType": "event", "eventCategory": "firstPage", "eventAction": "enter", "eventLabel": "Fall Campaign"}
{ "timestamp": 1496767740000, "hitType": "event", "eventCategory": "browseProduct", "eventAction": "List", "eventLabel": "Kids wear"}
{ "timestamp": 1496767750000, "hitType": "event", "eventCategory": "ViewReviews", "eventAction": "popup", "eventLabel": "Kids wear"}
