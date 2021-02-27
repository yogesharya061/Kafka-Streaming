/*
This is a Kafka Avro Producer program which is responsible for generating Avro
serialized messages to the kafka topic.

Steps to run the program:
    * Run the SetUpScripts/zookeeper-starts.cmd  --> To start Zookeeper server
    * Run the SetUpScripts/kafka-server.cmd      --> To start the Kafka server
    * Run the SetUpScripts/schema-registry.cmd   --> To start the Schema Registry server
    * Run the SetUpScripts/kafka-topic.cmd       --> To create a kafka topic
    * Run this Kafka Avro Producer Program

Learning: HandsetEnvelope class gets generated automatically when we build the code first time
          after placing avro schema under resources/avro directory.
          This class will be generated under Target folder and it will be available inside your final jar.
          Adding HandsetEnvelope class code separately inside this project folder for reference.
*/

package org.tech.kafkastreams;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tech.schema.handset.HandsetEnvelope;

import java.util.Properties;

public class KafkaAvroProducer {
    private static final Logger logger = LogManager.getLogger(KafkaAvroProducer.class);

    public static HandsetEnvelope createMockHandsetDetails(){
        /*
        This function helps to generate Avro data based on avro schema class HandsetEnvelope.
         */
        HandsetEnvelope envelope = new HandsetEnvelope();
        envelope.setIndex(5455789);
        envelope.setClosed(3099066175239L);
        envelope.setContract(2199356);
        envelope.setCustomer(9884795);
        envelope.setModel(5555);
        return envelope;
    }

    public static void main(String[] args) {
        logger.info("*** Starting Kafka Avro Producer Application ***");
        // Setting up properties for the Kafka Producer Application.
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Kafka Avro Producer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        // Creating Avro record by calling createMockHandsetDetails method.
        HandsetEnvelope handset_data = createMockHandsetDetails();
        Integer handset_key = handset_data.getIndex();

        // Creating KafkaProducer object to formulate the avro record and make it ready to send.
        KafkaProducer<Integer, HandsetEnvelope> handset_producer = new KafkaProducer<>(props);
        ProducerRecord<Integer, HandsetEnvelope> handset_record = new ProducerRecord<>("handset-topic", handset_key, handset_data);

        // Send the avro record to kafka topic..
        handset_producer.send(handset_record);
        logger.info("*** Shutting down the Producer Application ***");
        handset_producer.flush();
        handset_producer.close();

    }
}
