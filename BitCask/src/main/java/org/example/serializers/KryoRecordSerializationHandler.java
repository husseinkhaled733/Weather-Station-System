package org.example.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.TaggedFieldSerializer;
import org.example.interfaces.RecordSerializationHandler;
import org.example.models.HintFileRecord;
import org.example.models.SegmentFileRecord;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;

public class KryoRecordSerializationHandler /*implements RecordSerializationHandler*/ {

    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryo.register(Instant.class, new KryoInstantSerializer());
        return kryo;
    });

    private static Kryo getKryo() {
        return kryoThreadLocal.get();
    }

    //    @Override
    public <K, V> byte[] serializeSegmentRecord(SegmentFileRecord<K, V> segmentFileRecord) {

        try (
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Output output = new Output(byteArrayOutputStream)
        ) {

            Kryo kryo = getKryo();

            kryo.writeObject(output, segmentFileRecord.getTimeStamp());

            byte[] keyBytes = serializeObject(segmentFileRecord.getKey());
            byte[] valueBytes = serializeObject(segmentFileRecord.getValue());

            assert keyBytes != null;
            assert valueBytes != null;

            output.writeInt(keyBytes.length);
            output.writeInt(valueBytes.length);

            output.writeBytes(keyBytes);
            output.writeBytes(valueBytes);

            output.flush();
            output.close();

            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    //    @Override
    public <K> byte[] serializeHintRecord(HintFileRecord<K> hintFileRecord) {

        try (
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Output output = new Output(byteArrayOutputStream)
        ) {

            Kryo kryo = getKryo();

            kryo.writeObject(output, hintFileRecord.getTimeStamp(), new KryoInstantSerializer());
            output.writeInt(hintFileRecord.getKeySize());
            output.writeInt(hintFileRecord.getValueSize());
            output.writeLong(hintFileRecord.getValuePos());
            kryo.writeClassAndObject(output, hintFileRecord.getKey());

            output.flush();
            output.close();

            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public  <T> byte[] serializeObject(T object) {
        try (
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Output output = new Output(byteArrayOutputStream)
        ) {

            Kryo kryo = getKryo();
            kryo.writeClassAndObject(output, object);

            output.flush();
            output.close();

            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private Object deserializeObjectWithoutType(byte[] bytes) {
        try (
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
                Input input = new Input(byteArrayInputStream)
        ) {
            return getKryo().readClassAndObject(input);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public <V> V deserializeObject(byte[] bytes) {
        return (V) deserializeObjectWithoutType(bytes);
    }


    public <T> T deserializeObjectWithType(byte[] bytes, Class<T> type) {
        try (
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
                Input input = new Input(byteArrayInputStream)
        ) {
            return getKryo().readObject(input, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
//    // for testing
//    public <K, V> SegmentFileRecord<K, V> deserialize(byte[] bytes) {
//        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
//             Input input = new Input(byteArrayInputStream)) {
//
//            Kryo kryo = getKryo();
//
//            Instant timeStamp = kryo.readObject(input, Instant.class, new KryoInstantSerializer());
//            int keySize = input.readInt();
//            int valueSize = input.readInt();
//            byte[] keyBytes = input.readBytes(keySize);
//            byte[] valueBytes = input.readBytes(valueSize);
//            K key = deserializeObject(keyBytes);
//            V value = deserializeObject(valueBytes);
//
//            return SegmentFileRecord.<K, V>builder()
//                    .timeStamp(timeStamp)
//                    .keySize(keySize)
//                    .valueSize(valueSize)
//                    .key(key)
//                    .value(value)
//                    .build();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
}
