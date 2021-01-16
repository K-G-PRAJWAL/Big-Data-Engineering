#This is an example script for setting up a Kafka Connect
#Task that uses subscribes to a topic and writes it to a 
#HDFS File sink.

#Make sure safe-mode is OFF in hadoop
sudo -u hdfs hdfs dfsadmin -safemode leave

#Create a directory receive files.
hadoop fs -mkdir /user/cloudera/spark-data-engg

#listing hdfs files 
hadoop fs -ls

#Command to run kafka-connect with a file source connector and a task.
#and also a hdfs sink
export CLASSPATH=/home/cloudera/spark-data-engg/includes/*

/usr/lib/kafka/bin/connect-standalone.sh \
/usr/lib/kafka/config/connect-standalone.properties \
/home/cloudera/spark-data-engg/connect-file-source.properties \
/home/cloudera/spark-data-engg/connect-hdfs-sink.properties

#Command to view sinked data
 hadoop fs -cat spark-data-engg/topics/file-source-test/partition=0/*

#--------------------------------------------------
#Content of connect-hdfs-sink.properties
#--------------------------------------------------

#A logical name for your task
name=demo-hdfs-sink

#Class of connector to use
connector.class=io.confluent.connect.hdfs.HdfsSinkConnector

#Topics to subscribe.
topics=file-source-test

#Number of parallel tasks to launch. Provides scalability
tasks.max=1

#HDFS URL to write the data to
hdfs.url=hdfs://localhost/user/cloudera/spark-data-engg
flush.size=3



