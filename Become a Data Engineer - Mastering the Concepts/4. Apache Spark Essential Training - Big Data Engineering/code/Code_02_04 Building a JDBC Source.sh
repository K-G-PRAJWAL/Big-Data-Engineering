#This is an example script for setting up a Kafka Connect
#Task that uses a JDBC Source to publish topics.



#Command to run kafka-connect with a JDBC Source connector and a task.
export CLASSPATH=/home/cloudera/spark-data-engg/includes/*

/usr/lib/kafka/bin/connect-standalone.sh \
/usr/lib/kafka/config/connect-standalone.properties \
/home/cloudera/spark-data-engg/connect-jdbc-source.properties 

#Command to subscribe and listen
/usr/lib/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--zookeeper localhost:2181 \
--topic jdbc-source-jdbc_source \
--from-beginning

#--------------------------------------------------
#Content of connect-jdbc-source.properties
#--------------------------------------------------
#name of the connector
name=demo-jdbc-source
#connector class to be used.
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
#JDBC connector URL for mysql. make sure the mysql driver is in classpath.
connection.url=jdbc:mysql://10.35.3.149:3306/jdbctest?user=cloudera&password=cloudera
#List of tables to publish. you can also use blacklists
table.whitelist=jdbc_source
#No. of parallel tasks. Ideally one per table.
tasks.max=1

#How frequently to poll the db for new records
poll.interval.ms=2000
#mode - incrementing or timestamp+incrementing
mode=incrementing
incrementing.column.name=TEST_ID

#topic name to be created. This will create a topic jdbc-source-jdbc_source
#with the database name appended.
topic.prefix=jdbc-source-

#------------------------------------------------------------------

#This is the sample structure of each record published.
{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "int32",
			"optional": false,
			"field": "TEST_ID"
		},
		{
			"type": "int64",
			"optional": false,
			"name": "org.apache.kafka.connect.data.Timestamp",
			"version": 1,
			"field": "TEST_TIMESTAMP"
		}],
		"optional": false,
		"name": "jdbc_source"
	},
	"payload": {
		"TEST_ID": 1,
		"TEST_TIMESTAMP": 1495379345000
	}
}
