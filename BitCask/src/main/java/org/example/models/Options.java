package org.example.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Options {
    /*
     * read_write -> to indicate that this process is a writer not just a reader , default = false
     * sync_on_put -> if you need to sync after every write , default = false
     * directory -> path of the directory to be the main bitcask store , default = /src/main/resources/bitcask-base-dir
     * max_segment_size -> max file size in order to close it , default = 256 kb
     *
     * notes:
     * only one process can open the bitcask store at time in write mode
     */


    @Builder.Default
    private boolean readWrite = false;

    @Builder.Default
    private boolean syncOnPut = false;


    @Builder.Default
    private int maxSegmentSize = 256 * 1024;

    @Builder.Default
    private int maxFilesToCompact = 10;

    @Builder.Default
    private String baseDir = System.getProperty("user.dir") + "/src/main/resources/";
}