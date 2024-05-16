package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException {

        final Properties properties = Utils.loadConfig(Utils.propertiesPath);

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            consumer.subscribe(List.of(Utils.TOPIC));
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(5000));

            for (ConsumerRecord<String, String> record : messages) {
                String key = record.key();
                String value = record.value();
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", Utils.TOPIC, key, value));
            }
            consumer.commitSync();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}