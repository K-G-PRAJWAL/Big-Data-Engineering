#This is an example script for setting up a Kafka Connect
#Task that uses a File Source to publish topics.



#Command to run kafka-connect with a file source connector and a task.
/usr/lib/kafka/bin/connect-standalone.sh \
/usr/lib/kafka/config/connect-standalone.properties \
/home/cloudera/spark-data-engg/tweet-file-source.properties

#Command to test topic
/usr/lib/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--zookeeper localhost:2181 \
--topic use-case-tweets \
--from-beginning

#--------------------------------------------------
#Content of tweet-file-source.properties
#--------------------------------------------------

#A logical name for your task
name=usecase-tweets-source

#Class of connector to use
connector.class=FileStreamSource

#Number of parallel tasks to launch. Provides scalability
tasks.max=1

#Local file to monitor for new input
file=/home/cloudera/spark-data-engg/source-file/tweets.txt

#Topic to publish data to.
topic=use-case-tweets


#------------------------------------------------------------------
#This is the sample contents of tweets.txt. These are click events
#---------------------------------------------------------------------
1496767720000,Your product is awesome.#cool. I really like it
1496767730000,I had a bad experience with your product. Wont buy again. #sorry
1496767740000,@President. You should use this product. Its really great.
1496767750000,I need some support help here.
1496767790000,Why cant we do improve this product to be better than the old one?