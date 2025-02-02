FROM openjdk:8u151-jdk-alpine3.7

RUN apk add --no-cache bash libc6-compat

WORKDIR /

COPY wait.sh wait.sh
COPY target/flink-kafka2postgres-1.0-SNAPSHOT-jar-with-dependencies.jar flink-processor.jar

CMD ./wait.sh -s -t 30 $ZOOKEEPER_SERVER -- ./wait.sh -s -t 30 $KAFKA_SERVER -- java -Xmx512m -jar flink-processor.jar