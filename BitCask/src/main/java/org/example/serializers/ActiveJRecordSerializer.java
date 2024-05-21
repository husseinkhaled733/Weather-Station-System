package org.example.serializers;

import io.activej.serializer.SerializerBuilder;

public class ActiveJRecordSerializer {
    private final SerializerBuilder serializerBuilder;

    public ActiveJRecordSerializer() {
        this.serializerBuilder = SerializerBuilder.create();
        registerDefaultSerializers();
    }

    private void registerDefaultSerializers() {
        // Register default serializers, such as Instant, if needed
        // For example:
        // serializerBuilder.with(Instant.class, (output, instant) -> output.writeLong(instant.toEpochMilli()),
        //         input -> Instant.ofEpochMilli(input.readLong()));
    }

    // You can add methods to register custom serializers if needed

//    public byte[] serialize(Record<K, V> record) {
//        return serializerBuilder.build().encode(record);
//    }
//
//    public Record<K, V> deserialize(byte[] bytes) {
//        return serializerBuilder.build().decode(bytes, Record.class);
//    }
}
