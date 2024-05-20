package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class RainDetector {
    public static void main(String[] args) {
        String input_topic = "weather_data";
        String output_topic = "rain_data";
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        Topology topology = new Topology();
        topology.addSource("Source", input_topic)
                .addProcessor("RainDetectorProcessor", RainDetectorProcessor::new, "Source")
                .addSink("Sink", output_topic, "RainDetectorProcessor");

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
