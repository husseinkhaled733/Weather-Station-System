package org.example.interfaces;

import org.example.models.HintFileRecord;
import org.example.models.SegmentFileRecord;

public interface RecordSerializationHandler {

    <K, V> byte[] serializeSegmentRecord(SegmentFileRecord<K, V> segmentFileRecord);

    <K> byte[] serializeHintRecord(HintFileRecord<K> hintFileRecord);

    <T> byte[] serializeObject(T object);

    <T> T deserializeObject(byte[] bytes);

    <K, V> SegmentFileRecord<K, V> deserialize(byte[] bytes);
}
