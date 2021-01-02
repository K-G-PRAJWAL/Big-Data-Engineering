

1. Make sure to setup the localhost file in your windows machine. This is required for all exercises. Instructions for the same are available in the example - "Building a Apache Spark Sink."

2. For all examples, replace the IP addresses in the code and examples with your VM's IP address. This includes IP addresses for MySQL and Kafka.

3. If you want to republish data with Kafka Connect, please delete the file /tmp/connect.offsets. This will remove all offsets maintained by Kafka connect and republish data from the first record. Similarly, you can delete any topic currently stored in Kafka with the following command. Please replace <<topic-name>> below with the actual topic name.

/usr/lib/kafka/bin/kafka-topics.sh --delete \
--zookeeper localhost:2181 \
--topic <<topic-name>>