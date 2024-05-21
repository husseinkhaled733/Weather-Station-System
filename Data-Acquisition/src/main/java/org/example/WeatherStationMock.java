package org.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;

public class WeatherStationMock {
    private static final Random random = new Random();
    private static final String KAFKA_HOST = "localhost:9094";
    private static final int LOW_BATTERY_PERCENTAGE = 30;
    private static final int MEDIUM_BATTERY_PERCENTAGE = 40;
    private static final int HIGH_BATTERY_PERCENTAGE = 30;
    private static final int MESSAGE_DROP_RATE_PERCENTAGE = 10;
    private static final String KAFKA_TOPIC = "weather_data";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_HOST);
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            long stationId = 2;
            long serialNumber = 1;

            while (true) {
                if (!shouldDropMessage()) {
                    String batteryStatus = generateBatteryStatus();
                    long statusTimestamp = System.currentTimeMillis() / 1000;
                    int humidity = generateRandomValue(0, 100);
                    int temperature = generateRandomValue(-50, 150);
                    int windSpeed = generateRandomValue(0, 100);

                    String message = generateMessage(stationId, serialNumber, batteryStatus, statusTimestamp, humidity, temperature, windSpeed);
                    ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, message);
                    System.out.println("Sending message: " + message);
                    System.out.println("partition: " + record.partition()+ " topic: " + record.topic());

                    producer.send(record);

                    serialNumber++;
                }

                try {
                    Thread.sleep(1000); // Wait for 1 second
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static boolean shouldDropMessage() {
        return random.nextInt(100) < MESSAGE_DROP_RATE_PERCENTAGE;
    }

    private static String generateBatteryStatus() {
        int rand = random.nextInt(100);
        if (rand < LOW_BATTERY_PERCENTAGE) {
            return "low";
        } else if (rand < LOW_BATTERY_PERCENTAGE + MEDIUM_BATTERY_PERCENTAGE) {
            return "medium";
        } else {
            return "high";
        }
    }

    private static int generateRandomValue(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }

    private static String generateMessage(long stationId, long serialNumber, String batteryStatus, long statusTimestamp, int humidity, int temperature, int windSpeed) {
        return String.format("{\"station_id\":%d,\"s_no\":%d,\"battery_status\":\"%s\",\"status_timestamp\":%d,\"weather\":{\"humidity\":%d,\"temperature\":%d,\"wind_speed\":%d}}",
                stationId, serialNumber, batteryStatus, statusTimestamp, humidity, temperature, windSpeed);
    }
}
