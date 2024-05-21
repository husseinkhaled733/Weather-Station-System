package org.example.serializers;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class InstantSerializer {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(ZoneId.of("UTC"));

    public static String instantToString(Instant instant) {
        if (instant == null) {
            throw new IllegalArgumentException("Instant cannot be null");
        }
        return FORMATTER.format(instant);
    }

    public static byte[] stringToBytes(String instantString) {
        if (instantString == null || instantString.isEmpty()) {
            throw new IllegalArgumentException("String cannot be null or empty");
        }
        return instantString.getBytes();
    }


    public static String bytesToString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Byte array cannot be null or empty");
        }
        return new String(bytes);
    }

    public static Instant stringToInstant(String instantString) {
        if (instantString == null || instantString.isEmpty()) {
            throw new IllegalArgumentException("String cannot be null or empty");
        }
        return Instant.from(FORMATTER.parse(instantString));
    }
}
