package org.example.models;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigInteger;
import java.time.Instant;

@Data
@Builder
@AllArgsConstructor
public class KeyDirEntry {
    private BigInteger fileId;
    private int valueSize;
    private long valuePos;
    private Instant timeStamp;
}
