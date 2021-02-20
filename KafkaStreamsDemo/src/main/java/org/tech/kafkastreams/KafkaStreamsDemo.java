package org.tech.kafkastreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.kafka.common.serialization.Serdes.String;
import java.util.Properties;

public class KafkaStreamsDemo {
    private static final Logger logger = LogManager.getLogger(KafkaStreamsDemo.class);

    public Topology buildTopology(){
        logger.info("*** Starting Kafka Streams Application ***");

        // Create object of Stream Builder Class for stream processing..
        final StreamsBuilder builder = new StreamsBuilder();

        // Create stream from kafka topic..
        KStream<String, String> input = builder.stream("kafka-streams-demo");

        // Print data to console..
        input.foreach((k, v) -> System.out.println(k + ": " + v));

        return builder.build();
    }

    public static void main(String[] args) {
        logger.info("*** Welcome to Kafka Streams Demo ***");

        // Creating class object..
        KafkaStreamsDemo demo = new KafkaStreamsDemo();

        // Setting up kafka streams properties..
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka_Stream_Demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass().getName());

        // Create Topology for the streaming job..
        Topology topology = demo.buildTopology();

        // Create kafka stream object using topology and properties..
        final KafkaStreams streams = new KafkaStreams(topology, props);

        // Start the stream..
        streams.start();

        /*
        To stop the application, just press stop button in Intellij or
        if you are running the application from command line then press ctrl+C
         */
    }
}
