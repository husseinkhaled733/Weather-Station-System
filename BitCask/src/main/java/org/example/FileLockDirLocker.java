package org.example;

import org.example.interfaces.IDirLocker;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.example.Constants.*;

public class FileLockDirLocker implements IDirLocker {

    private static final String LOCK_FILE_PREFIX = "BITCASK_LOCK_";

    @Override
    public boolean acquireWriteLock(String directory) {
        try {
            Path lockFilePath = Paths.get(directory + LOCK_FILES_DIRECTORY + "BITCASK_LOCK_" + directory);

            if (!Files.exists(lockFilePath))
                Files.createFile(lockFilePath);

            try (FileChannel channel = FileChannel.open(lockFilePath, StandardOpenOption.APPEND)) {
                FileLock lock = channel.tryLock();
                return lock != null;
            }

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void releaseWriteLock(String directory) {
        try {
            Path lockFilePath = Paths.get(directory + LOCK_FILES_DIRECTORY + "BITCASK_LOCK_" + directory);
            if (Files.exists(lockFilePath))
                Files.delete(lockFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
