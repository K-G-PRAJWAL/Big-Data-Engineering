# Kafka-Hudi Spark Streaming Project

This project makes use of Spark streaming to consume the output from the a Kafka producer broker into a Hudi data lake.

Requirement:
Every 5 seconds a streaming service will push data to a Kafka producer broker on port 9092. The payload will consist of employee information having employee_id as the primary key, timestamp of creation along with other data. This topic will be subscribed by a Spark Streaming service to consume the payload from Kafka broker and write the data into a Hudi Datalake with an upsert on the emp_id and de-dupe on the timestamp key.  
1. Setup a Spark Environment:
   1. Install Spark: https://spark.apache.org/downloads.html
   2. Install Java 8 - https://www.oracle.com/java/technologies/downloads/#java8-windows
   3. Add Java 8 to "Path" - `C:\Program Files\Java\jdk1.8.0_301\bin`
   4. Add Spark to "Path" - `E:\DATA\Apps\spark-3.3.2-bin-hadoop3\bin`
   5. Download winutils + hadoop.dll files from - https://github.com/kontext-tech/winutils/tree/master/hadoop-3.3.0/bin
   6. Add `HADDOP_HOME` to env. variables as `E:\hadoop` where the folder `hadoop` has a folder `bin` which has the `winutils.exe` and `hadoop.dll` files.
      1. (If the program doesn't execute, try copying the hadoop.dll to the System32 folder in Windows)

2. Setup Docker environment:
   1. Docker will be required in this project to setup a zookeeper and a kafka broker service.
   2. Install Docker from: https://www.docker.com/products/docker-desktop/
   3. Start the docker container: `docker-compose up --build` - Starts the Zookeeper service and the Kaka broker
      1. Zookeeper Image: https://hub.docker.com/r/confluentinc/cp-zookeeper
      2. Kafka Image: https://hub.docker.com/r/confluentinc/cp-kafka

3. Install the requirements provided in the `requirements.txt` file.
4. Start the Kafka producer to generate dummy data: `python kafka-producer.py`
5. Start the Streaming Data Consumption Service `python hudi-kafka.py`

Spark jobs can be viewed on : http://localhost:4040/jobs/