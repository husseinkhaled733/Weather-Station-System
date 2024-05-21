package org.example.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.nio.file.Path;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SegmentFile {
    private BigInteger fileId; // a sequence number to keep order of creation of files
    private Path filePath;

    public SegmentFile(BigInteger fileId, String dir) {
        this.fileId = fileId;
        this.filePath = Path.of(dir + "segment_file_" + fileId.toString());
    }
}
