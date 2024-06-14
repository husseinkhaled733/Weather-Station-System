package org.example;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.TaggedFieldSerializer;
import lombok.SneakyThrows;
import org.example.interfaces.RecordSerializationHandler;
import org.example.models.Options;
import org.example.serializers.KryoInstantSerializer;
import org.example.serializers.KryoRecordSerializationHandler;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {



//        testRecovery();
//        testLocks();
//        writeManyAndReadMany();

//        BitCask<Integer, String> bitCask = new BitCask<>(Options.builder()
//                .maxSegmentSize(1024)
//                .build());
//
//        Kryo kryo = new Kryo();
//        kryo.setRegistrationRequired(false);
//        kryo.setDefaultSerializer(TaggedFieldSerializer.class);
//        kryo.register(Instant.class , new KryoInstantSerializer());
////
//        KryoRecordSerializationHandler serializationHandler = new KryoRecordSerializationHandler();
//
//        Message message = Message.builder()
//                .battery_status("low")
//                .s_no(100)
//                .station_id(1)
//                .status_timestamp(1000)
//                .weather(Weather
//                        .builder()
//                        .humidity(10)
//                        .temperature(37)
//                        .wind_speed(120)
//                        .build())
//                .build();

////

//        System.out.println(message);
//        var bytes = serializationHandler.serializeObject(message);
//        System.out.println(bytes.length);
//        Message deSegFile = serializationHandler.deserializeObject(bytes);
//        System.out.println(deSegFile);

//
//        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//             Output output = new Output(byteArrayOutputStream)) {
//
//            kryo.writeObject(output, Instant.MAX);
//            output.flush();
//
//            System.out.println(byteArrayOutputStream.size());
//
//            var vv = byteArrayOutputStream.toByteArray();
//            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(vv);
//            Input input = new Input(byteArrayInputStream);
//            Instant timeStamp = kryo.readObject(input, Instant.class);
//            System.out.println(timeStamp);
//        }

    }

    @SneakyThrows
    private static void testLocks() {
        BitCask<Integer , String> cask = new BitCask<>(Options.builder()
                .maxSegmentSize(1024)
                .maxFilesToCompact(10)
                .build());

        cask.put(0, "anwar" + 0);

        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            int finalI = i;
            Thread t = new Thread(() -> {
                cask.put(finalI, generateRandomString(100) + finalI);
            });
            t.start();
            threads.add(t);
        }

        for (var t : threads) {
            t.join();
        }



//        try {
//            Thread.sleep(3000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

//        new Thread(() -> {
//            cask.get(0).ifPresent(System.out::println);
//        }).start();

    }

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random RANDOM = new Random();

    public static String generateRandomString(int length) {
        return RANDOM.ints(length, 0, CHARACTERS.length())
                .mapToObj(CHARACTERS::charAt)
                .map(Object::toString)
                .collect(Collectors.joining());
    }

    private static void testRecovery() throws InterruptedException {
        BitCask<Integer, String> bitCask = new BitCask<>(Options.builder()
                .maxFilesToCompact(1)
                .maxSegmentSize(32 * 3)
                .build());
//
//        bitCask.put(2, "mariem");
//        Thread.sleep(3000);

//        bitCask.put(3, "anwarr");
//        Thread.sleep(3000);
//
//        bitCask.put(4, "gerges");

//        bitCask.put(5, "hamada");

        bitCask.get(2).ifPresent(System.out::println);
        bitCask.get(3).ifPresent(System.out::println);
        bitCask.get(4).ifPresent(System.out::println);
        bitCask.get(5).ifPresent(System.out::println);
    }

    public static byte[] serializeToByteArray(Object obj) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(obj);
            objectOutputStream.close();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void calcTime(Runnable function) {
        long start = System.currentTimeMillis();
        function.run();
        System.out.println(System.currentTimeMillis() - start);
    }

    static void writeOnceAndRead() {
        BitCask<Integer, String> bitCask = new BitCask<>();
        new Thread(() -> {
            bitCask.put(1, "anwar");
        }).start();
        new Thread(() -> {
            bitCask.put(2, "mariam");
        }).start();
        new Thread(() -> {
            bitCask.put(3, "hussien");
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            bitCask.get(1).ifPresent(System.out::println);
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            bitCask.get(2).ifPresent(System.out::println);
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            bitCask.get(3)/*.ifPresent(System.out::println)*/;
        }).start();

    }

    static void writeManyAndReadMany() throws InterruptedException {
        BitCask<Integer, String> bitCask = new BitCask<>(
                Options.builder()
                        .maxSegmentSize(45)
                        .maxFilesToCompact(100)
                        .build()
        );


        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i < 300; i++) {
            int finalI = i;
            Thread t = new Thread(() -> {
                bitCask.put(finalI, "anwar" + finalI);
            });
            t.start();
            threads.add(t);
        }


        Thread.sleep(1000);
        for (int i = 0; i < 200; i++) {
            int finalI = i;
            Thread t = new Thread(() -> {
                        bitCask.get(finalI % 20).ifPresent(System.out::println);
            });
            t.start();
            threads.add(t);
        }

        for (var t : threads) {
            t.join();
        }


//        System.out.println(bitCask.listKeys());
    }
}