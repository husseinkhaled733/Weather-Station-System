package org.example.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.time.Instant;

@Data
@Builder
@AllArgsConstructor
public class HintFileRecord<K> implements Serializable {
    private Instant timeStamp;
    private int keySize;
    private int valueSize;
    private long valuePos;
    private K key;
}
