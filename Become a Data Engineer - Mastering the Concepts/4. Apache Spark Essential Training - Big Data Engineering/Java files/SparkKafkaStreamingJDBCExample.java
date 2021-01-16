
package com.lynda.course.sparkbde;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class SparkKafkaStreamingJDBCExample {
	public static void main(String[] args) {
		
		//Setup log levels so there is no slew of info logs
		//Logger.getLogger("org").setLevel(Level.ERROR);
		
		//Start a spark instance and get a context
		SparkConf conf = new SparkConf()
						.setMaster("local[2]")
						.setAppName("Study Spark");
		
		//Setup a streaming context.
		JavaStreamingContext streamingContext 
			= new JavaStreamingContext(conf, Durations.seconds(3));
		
		//Create a map of Kafka params
		Map<String, Object> kafkaParams 
				= new HashMap<String,Object>();
		//List of Kafka brokers to listen to.
		kafkaParams.put("bootstrap.servers", 
							"10.35.3.149:9092");
		kafkaParams.put("key.deserializer", 
							StringDeserializer.class);
		kafkaParams.put("value.deserializer", 
							StringDeserializer.class);
		kafkaParams.put("group.id", 
						"use_a_separate_group_id_for_each_stream");
		//Do you want to start from the earliest record or the latest?
		kafkaParams.put("auto.offset.reset", 
						"earliest");
		kafkaParams.put("enable.auto.commit", false);

		//List of topics to listen to.
		Collection<String> topics 
			= Arrays.asList("jdbc-source-jdbc_source");

		//Create a Spark DStream with the kafka topics.
		final JavaInputDStream<ConsumerRecord<String, String>> stream =
		  KafkaUtils.createDirectStream(
		    streamingContext,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(
		    								topics, kafkaParams)
		  );


		//Setup a processing map function that only returns the payload.
		JavaDStream<String> retval= stream.map(
				  new Function<ConsumerRecord<String, String>,  String>() {

					private static final long serialVersionUID = 1L;

					public String call(ConsumerRecord<String, String> record) {
				      return record.value();
				    }
				  });
		//print the payload.
		retval.print();
		
		//Start streaming.
		streamingContext.start();
		
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//streamingContext.close();
		
		//Keep the program alive.
		while( true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}


}
