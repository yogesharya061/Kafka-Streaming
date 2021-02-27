/*
This is a Kafka Avro Consumer program which is responsible for retrieving the Avro
serialized messages from the kafka topic and de-serializing them to display on console.

Mandatory : Kafka Topic must be available before running this application.
Steps to run the program:
    * Run the SetUpScripts/zookeeper-starts.cmd  --> To start Zookeeper server
    * Run the SetUpScripts/kafka-server.cmd      --> To start the Kafka server
    * Run the SetUpScripts/schema-registry.cmd   --> To start the Schema Registry server
    * Run this Kafka Avro Consumer Program

Learning: HandsetEnvelope class gets generated automatically when we build the code first time
          after placing avro schema under resources/avro directory.
          This class will be generated under Target folder and it will be available inside your final jar.
          Added HandsetEnvelope class code separately in KafkaAvroProducer directory for reference.
*/

package org.tech.kafkastreams;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tech.schema.handset.HandsetEnvelope;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumer {
    private static final Logger logger = LogManager.getLogger(KafkaAvroConsumer.class);

    public static void main(String[] args) {
        logger.info("*** Starting Kafka Avro Consumer Application ***");
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Kafka Avro Consumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-avro-consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        // Creating Consumer object to subscribe a kafka topic.
        KafkaConsumer<Integer, HandsetEnvelope> handset_consumer = new KafkaConsumer<>(props);
        handset_consumer.subscribe(Collections.singleton("handset-topic"));

        logger.info("Waiting for the data...");
        try {
            while (true) {
                // Polling records from kafka topic using consumer object.
                ConsumerRecords<Integer, HandsetEnvelope> records = handset_consumer.poll(1000);

                for (ConsumerRecord<Integer, HandsetEnvelope> record : records) {
                    HandsetEnvelope handset = record.value();
                    Integer key = record.key();
                    System.out.println(key+":"+handset);
                }
            }
        }catch(Exception ex){
            logger.info("*** Some error in the process. Please debug the issue ***");
            ex.printStackTrace();
        }finally{
            logger.info("*** Shutting down the consumer application ***");
            handset_consumer.close();
        }
        /*
        To stop the application, just press stop button in Intellij or
        if you are running the application from command line then press ctrl+C
         */
    }
}
