package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.bitcask.BitCaskWriter;
import org.example.parquet.ParquetWriter;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class BaseCentralStation {
    public static void main(String[] args) throws IOException {

        final Properties properties = Utils.loadConfig(Utils.propertiesPath);

        ParquetWriter parquetWriter = new ParquetWriter();
        BitCaskWriter bitCaskWriter = new BitCaskWriter();

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            consumer.subscribe(List.of(Utils.TOPIC));

            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
                new Thread(() -> parquetWriter.addMessagesUtil(messages)).start();
                new Thread(() -> bitCaskWriter.writeRecords(messages)).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}