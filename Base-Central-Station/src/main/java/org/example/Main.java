package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.dto.Message;
import org.example.parquet.ParquetWriter;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException {


        final Properties properties = Utils.loadConfig(Utils.propertiesPath);

        ParquetWriter parquetWriter = new ParquetWriter();

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            consumer.subscribe(List.of(Utils.TOPIC));

            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
                new Thread(() -> parquetWriter.addMessagesUtil(messages)).start();
                // Todo: call bitcask thread
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}