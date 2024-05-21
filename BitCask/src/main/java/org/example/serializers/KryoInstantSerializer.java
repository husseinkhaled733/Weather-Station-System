package org.example.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.time.Instant;

public class KryoInstantSerializer extends Serializer<Instant> {
    @Override
    public void write(Kryo kryo, Output output, Instant instant) {
        output.writeLong(instant.getEpochSecond());
        output.writeInt(instant.getNano());
    }

    @Override
    public Instant read(Kryo kryo, Input input, Class<Instant> type) {
        long seconds = input.readLong();
        long nanos = input.readInt();
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
