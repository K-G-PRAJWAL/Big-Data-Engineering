#This is an example script for setting up a Kafka Connect
#Task that uses a JDBC Source to publish topics.



#Command to run kafka-connect with a JDBC Source connector and a task.
export CLASSPATH=/home/cloudera/spark-data-engg/includes/*

/usr/lib/kafka/bin/connect-standalone.sh \
/usr/lib/kafka/config/connect-standalone.properties \
/home/cloudera/spark-data-engg/connect-eu-sales-data.properties 

#Command to test topic
/usr/lib/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--zookeeper localhost:2181 \
--topic use-case-book_sales \
--from-beginning

#--------------------------------------------------
#Content of connect-eu-sales-data.properties
#--------------------------------------------------
#name of the connector
name=use-case-eu-sales
#connector class to be used.
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
#JDBC connector URL for mysql. make sure the mysql driver is in classpath.
connection.url=jdbc:mysql://10.35.3.149:3306/eu_sales?user=cloudera&password=cloudera
#List of tables to publish. you can also use blacklists
table.whitelist=book_sales
#No. of parallel tasks. Ideally one per table.
tasks.max=1

#How frequently to poll the db for new records
poll.interval.ms=2000
#mode - incrementing or timestamp+incrementing
mode=incrementing
incrementing.column.name=ID

#topic name to be created. This will create a topic jdbc-source-jdbc_source
#with the database name appended.
topic.prefix=use-case-

#------------------------------------------------------------------

#This is the sample structure of each record published.
{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "int32",
			"optional": false,
			"field": "ID"
		},
		{
			"type": "string",
			"optional": true,
			"field": "BOOK_NAME"
		},
		{
			"type": "int64",
			"optional": false,
			"name": "org.apache.kafka.connect.data.Timestamp",
			"version": 1,
			"field": "SALES_DATE"
		},
		{
			"type": "double",
			"optional": true,
			"field": "ORDER_AMOUNT"
		}],
		"optional": false,
		"name": "book_sales"
	},
	"payload": {
		"ID": 2,
		"BOOK_NAME": "Spark Guide",
		"SALES_DATE": 1495994802000,
		"ORDER_AMOUNT": 32.0
	}
}