package org.example;

import org.apache.commons.lang3.tuple.Pair;
import org.example.interfaces.FoldFunction;
import org.example.interfaces.IBitCask;
import org.example.interfaces.IDirLocker;
import org.example.interfaces.RecordSerializationHandler;
import org.example.models.*;
import org.example.serializers.KryoRecordSerializationHandler;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.Entry;
import static org.example.Constants.*;

public class BitCask<K, V> implements IBitCask<K, V> {

    private final Options options;
    private final IDirLocker dirLocker;

    private final Map<K, KeyDirEntry> keyDir;
    private final KryoRecordSerializationHandler recordSerializationHandler;

    private SegmentFile activeFile;
    private FileChannel activeFileChannel;

    private BigInteger currentId;
    private final AtomicInteger oldFilesCount;

    private ReaderLocksHandler readersLocks;

    /*
     * - if the current process is a writer we check if the directory is not locked by other processes
     *   then it will be closed with an exit code 1
     * - if the current directory is not empty then we initiate the recovery
     * */
    public BitCask(Options options) {
        this.options = options;
        this.dirLocker = new FileLockDirLocker();
        this.keyDir = new ConcurrentHashMap<>();
        this.recordSerializationHandler = new KryoRecordSerializationHandler();
        this.currentId = BigInteger.ZERO;
        this.oldFilesCount = new AtomicInteger(0);
        this.readersLocks = new ReaderLocksHandler();

        createDefaultDirectories();
        handleDirectoryLock();
        initiateRecovery();
        resetActiveFile();
    }

    public BitCask() {
        this(new Options());
    }

    private void initiateRecovery() {
        Path segmentFilesDir = Path.of(options.getBaseDir() + SEGMENT_FILES_DIRECTORY);
        Path hintFilesDir = Path.of(options.getBaseDir() + HINT_FILES_DIRECTORY);

        // segment , hint
        Map<BigInteger, Pair<Path, Path>> files = new HashMap<>();

        try (
                Stream<Path> segFilesPathStream = Files.walk(segmentFilesDir);
                Stream<Path> hintFilesPathStream = Files.walk(hintFilesDir);
                Stream<Path> deletedFilesStream = Files.walk(segmentFilesDir);
        ) {
            segFilesPathStream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith("segment_file_"))
                    .forEach(path -> {
                        var fileId = new BigInteger(
                                path.getFileName()
                                        .toString()
                                        .split("_")[2]
                        );

                        if (files.containsKey(fileId))
                            files.put(fileId, Pair.of(
                                    path,
                                    files.get(fileId).getRight()
                            ));
                        else files.put(fileId, Pair.of(path, null));
                    });

            hintFilesPathStream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith("hint_file_"))
                    .forEach(path -> {
                        var fileId = new BigInteger(
                                path.getFileName()
                                        .toString()
                                        .split("_")[2]
                        );

                        if (files.containsKey(fileId))
                            files.put(fileId, Pair.of(
                                    files.get(fileId).getLeft(),
                                    path
                            ));
                        else files.put(fileId, Pair.of(null, path));
                    });

            deletedFilesStream
                    .filter(Files::isRegularFile)
                    .filter(path -> !path.getFileName().toString().startsWith("segment_file_") &&
                            !path.getFileName().toString().startsWith("hint_file_")
                    )
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });


            files.entrySet().forEach(System.out::println);

        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Map.Entry<BigInteger, Pair<Path, Path>> entry : files.entrySet()) {
            BigInteger fileId = entry.getKey();
            boolean hasHintFile = entry.getValue().getRight() != null;

            try (
                    FileChannel hintFileChannel = hasHintFile ? FileChannel.open(
                            entry.getValue().getRight(),
                            StandardOpenOption.READ
                    ) : null;

                    FileChannel segmentFileChannel = FileChannel.open(
                            entry.getValue().getLeft(),
                            StandardOpenOption.READ
                    )
            ) {

                if (hasHintFile) {

                    while (hintFileChannel.position() < hintFileChannel.size()) {

                        try {
                            final int CONST_BUFFER_SIZE =
                                    TIMESTAMP_SERIALIZATION_SIZE +
                                            2 * INT_SERIALIZATION_SIZE +
                                            LONG_SERIALIZATION_SIZE;

                            ByteBuffer constFields = ByteBuffer.allocate(CONST_BUFFER_SIZE);

                            int bytesRead = hintFileChannel.read(constFields);

                            if (bytesRead != CONST_BUFFER_SIZE) {
                                System.out.println("Error: can't read record");
                                break;
                            }

                            constFields.flip();

                            byte[] timeStampBytes = new byte[TIMESTAMP_SERIALIZATION_SIZE];
                            constFields.get(timeStampBytes);

                            Instant timeStamp = recordSerializationHandler.deserializeObjectWithType(
                                    timeStampBytes,
                                    Instant.class
                            );
                            int keySize = constFields.getInt();
                            int valueSize = constFields.getInt();
                            long valuePos = constFields.getLong();

                            ByteBuffer variableFields = ByteBuffer.allocate(keySize);
                            bytesRead = hintFileChannel.read(variableFields);
                            if (bytesRead != keySize) {
                                System.out.println("can't read record");
                                break;
                            }

                            variableFields.flip();
                            byte[] keyBytes = new byte[keySize];
                            variableFields.get(keyBytes);

                            K key = recordSerializationHandler.deserializeObject(keyBytes);

                            keyDir.put(key, KeyDirEntry.builder()
                                    .fileId(fileId)
                                    .timeStamp(timeStamp)
                                    .valuePos(valuePos)
                                    .valueSize(valueSize)
                                    .build());

                        } catch (Exception e) {
                            break;
                        }
                    }

                } else {

                    System.out.println(segmentFileChannel.size());

                    while (segmentFileChannel.position() < segmentFileChannel.size()) {

                        try {
                            long valuePos = segmentFileChannel.position();
                            final int CONST_BUFFER_SIZE = TIMESTAMP_SERIALIZATION_SIZE + 2 * INT_SERIALIZATION_SIZE;

                            ByteBuffer constFields = ByteBuffer.allocate(CONST_BUFFER_SIZE);
                            int bytesRead = segmentFileChannel.read(constFields);

                            if (bytesRead != CONST_BUFFER_SIZE) {
                                System.out.println("can't read record");
                                break;
                            }

                            constFields.flip();
                            byte[] timeStampBytes = new byte[TIMESTAMP_SERIALIZATION_SIZE];
                            constFields.get(timeStampBytes);

                            Instant timeStamp = recordSerializationHandler.deserializeObjectWithType(
                                    timeStampBytes,
                                    Instant.class
                            );
                            int keySize = constFields.getInt();
                            int valueSize = constFields.getInt();

                            ByteBuffer variableFields = ByteBuffer.allocate(keySize);
                            bytesRead = segmentFileChannel.read(variableFields);
                            if (bytesRead != keySize) {
                                System.out.println("can't read record");
                                break;
                            }

                            variableFields.flip();
                            byte[] keyBytes = new byte[keySize];
                            variableFields.get(keyBytes);

                            K key = recordSerializationHandler.deserializeObject(keyBytes);

                            keyDir.put(key, KeyDirEntry.builder()
                                    .fileId(fileId)
                                    .timeStamp(timeStamp)
                                    .valuePos(valuePos)
                                    .valueSize(valueSize)
                                    .build());

                            segmentFileChannel.position(segmentFileChannel.position() + valueSize);
                        } catch (Exception e) {
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    private void handleDirectoryLock() {
        if (!options.isReadWrite()) return;

        boolean acquiredLock = dirLocker.acquireWriteLock(options.getBaseDir());
        if (!acquiredLock) {
            System.out.println("Directory is locked by another process");
            System.exit(1);
        }
    }

    private void createDefaultDirectories() {
        Path segFilesDir = Paths.get(options.getBaseDir() + SEGMENT_FILES_DIRECTORY);
        Path hintFilesDir = Paths.get(options.getBaseDir() + HINT_FILES_DIRECTORY);
        Path lockFilesDir = Paths.get(options.getBaseDir() + LOCK_FILES_DIRECTORY);

        try {
            if (!Files.exists(segFilesDir))
                Files.createDirectories(segFilesDir);

            if (!Files.exists(hintFilesDir))
                Files.createDirectories(hintFilesDir);

            if (!Files.exists(lockFilesDir))
                Files.createDirectories(lockFilesDir);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<V> get(K key) {

        KeyDirEntry entry = keyDir.get(key);

        if (entry == null)
            return Optional.empty();

        readersLocks.acquireReadLockForFile(entry.getFileId());

        SegmentFile segmentFile = getSelectedFileToRead(entry);

        try (
                FileChannel channel = FileChannel.open(
                        segmentFile.getFilePath(),
                        StandardOpenOption.READ
                );
        ) {

            channel.position(entry.getValuePos());

            // time_stamp , key_size and value_size
            ByteBuffer constFields = ByteBuffer.allocate(TIMESTAMP_SERIALIZATION_SIZE + 2 * INT_SERIALIZATION_SIZE);
            channel.read(constFields);

            constFields.flip();
            constFields.position(TIMESTAMP_SERIALIZATION_SIZE);

            int keySize = constFields.getInt();
            int valueSize = constFields.getInt();

            ByteBuffer variableFields = ByteBuffer.allocate(keySize + valueSize);
            channel.read(variableFields);

            variableFields.flip();
            variableFields.position(keySize);

            byte[] valueBytes = new byte[valueSize];
            variableFields.get(valueBytes);

            readersLocks.releaseReadLockForFile(entry.getFileId());
            return Optional.of(recordSerializationHandler.deserializeObject(valueBytes));
        } catch (IOException e) {
            e.printStackTrace();
        }

        readersLocks.releaseReadLockForFile(entry.getFileId());
        return Optional.empty();
    }

    private SegmentFile getSelectedFileToRead(KeyDirEntry entry) {

        if (Objects.equals(entry.getFileId(), activeFile.getFileId())) {
            if (!options.isSyncOnPut()) {
                try {
                    activeFileChannel.force(true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return activeFile;

        }
        return new SegmentFile(
                entry.getFileId(),
                options.getBaseDir() + SEGMENT_FILES_DIRECTORY
        );
    }

    private void resetActiveFile() {
        activeFile = new SegmentFile(
                currentId,
                options.getBaseDir() + SEGMENT_FILES_DIRECTORY
        );

        currentId = currentId.add(BigInteger.ONE);

        try {
            activeFileChannel = FileChannel.open(activeFile.getFilePath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void put(K key, V value) {
        SegmentFileRecord<K, V> record = SegmentFileRecord
                .<K, V>builder()
                .timeStamp(Instant.now())
                .key(key)
                .value(value)
                .build();

        byte[] serializedRecord = recordSerializationHandler.serializeSegmentRecord(record);
        ByteBuffer buffer = ByteBuffer.wrap(serializedRecord);

        try {
            if (activeFileChannel.size() + serializedRecord.length > options.getMaxSegmentSize()) {
                oldFilesCount.incrementAndGet();
                resetActiveFile();

                if (oldFilesCount.get() >= options.getMaxFilesToCompact()) {
                    System.out.println("Num of Files: " + oldFilesCount.get());
                    System.out.println("Num of FileIds: " + keyDir
                            .values()
                            .stream()
                            .map(KeyDirEntry::getFileId)
                            .collect(Collectors.toSet())
                            .size());

                    oldFilesCount.addAndGet(-options.getMaxFilesToCompact());
                    System.out.println("Last file id (in put): " + activeFile.getFileId());
                    new Thread(() -> merge(new BigInteger(activeFile.getFileId().toString()))).start();
                }

            }

            long position = activeFileChannel.size();

            while (buffer.hasRemaining()) {
                activeFileChannel.write(buffer);
            }

            if (options.isSyncOnPut()) {
                try {
                    activeFileChannel.force(true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            KeyDirEntry keyDirEntry = KeyDirEntry.builder()
                    .fileId(activeFile.getFileId())
                    .valueSize(record.getValueSize())
                    .valuePos(position)
                    .timeStamp(record.getTimeStamp())
                    .build();

            keyDir.put(key, keyDirEntry);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void merge() {
        resetActiveFile();
        merge(activeFile.getFileId());
    }

    private synchronized void merge(BigInteger lastActiveFileId) {
        System.out.println("Merging " + Thread.currentThread());

        System.out.println("Current File id: " + lastActiveFileId);

        Map<K, KeyDirEntry> tempMap = new HashMap<>();

        // built tempMap
        keyDir.entrySet()
                .stream()
                .parallel()
                .filter(entry -> entry.getValue().getFileId().compareTo(lastActiveFileId) < 0)
                .forEach(entry -> tempMap.put(entry.getKey(), entry.getValue()));

        // getting files to be deleted
        List<SegmentFile> filesToDelete = tempMap.values()
                .stream()
                .parallel()
                .filter(entry -> entry.getFileId().compareTo(BigInteger.ZERO) != 0)
                .map(entry -> new SegmentFile(entry.getFileId(), options.getBaseDir() + SEGMENT_FILES_DIRECTORY))
                .peek(segmentFile -> System.out.println("Files to deleted : " + segmentFile.getFileId()))
                .toList();

        if (filesToDelete.isEmpty()) {
            System.out.println("Done Merging " + Thread.currentThread() + "\n");
            return;
        }

        // writing the mergedFile and hintFile
        Path mergedFilePath = Path.of(options.getBaseDir() + SEGMENT_FILES_DIRECTORY).resolve("segment_file_0");
        Path hintFilePath = Path.of(options.getBaseDir() + HINT_FILES_DIRECTORY + "hint_file_0");
        Path tempMergeFilePath = Path.of(options.getBaseDir() + SEGMENT_FILES_DIRECTORY + UUID.randomUUID());

        try (
                FileChannel mergedFileWriterChannel = FileChannel.open(
                        tempMergeFilePath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );

                FileChannel hintFileWriterChannel = FileChannel.open(
                        hintFilePath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );
        ) {

            List<Entry<K, KeyDirEntry>> updatedEntries = new ArrayList<>();

            for (Entry<K, KeyDirEntry> mapEntry : tempMap.entrySet()) {

                K key = mapEntry.getKey();
                KeyDirEntry entry = mapEntry.getValue();
                SegmentFile segmentFile = getSelectedFileToRead(entry);
                if (segmentFile == null)
                    continue;

                try (FileChannel readerChannel = FileChannel.open(segmentFile.getFilePath(), StandardOpenOption.READ)) {

                    readerChannel.position(entry.getValuePos());

                    ByteBuffer constFields = ByteBuffer.allocate(TIMESTAMP_SERIALIZATION_SIZE + 2 * INT_SERIALIZATION_SIZE);
                    readerChannel.read(constFields);
                    constFields.flip();

                    long position = mergedFileWriterChannel.size();

                    mergedFileWriterChannel.write(ByteBuffer.wrap(constFields.array().clone()));
                    constFields.position(TIMESTAMP_SERIALIZATION_SIZE);

                    int keySize = constFields.getInt();
                    int valueSize = constFields.getInt();

                    ByteBuffer variableFields = ByteBuffer.allocate(keySize + valueSize);
                    readerChannel.read(variableFields);

                    variableFields.flip();
                    mergedFileWriterChannel.write(ByteBuffer.wrap(variableFields.array().clone()));

                    HintFileRecord<K> hintFileRecord = HintFileRecord.<K>builder()
                            .timeStamp(Instant.now())
                            .keySize(keySize)
                            .valuePos(position)
                            .key(key)
                            .build();

                    byte[] hintFileBytes = recordSerializationHandler.serializeHintRecord(hintFileRecord);
                    hintFileWriterChannel.write(ByteBuffer.wrap(hintFileBytes));

                    mergedFileWriterChannel.force(true);
                    hintFileWriterChannel.force(true);

                    updatedEntries.add(Pair.of(
                            key,
                            KeyDirEntry.builder()
                                    .fileId(BigInteger.ZERO)
                                    .valueSize(valueSize)
                                    .valuePos(position)
                                    .timeStamp(Instant.now())
                                    .build()
                    ));
                }
            }

            updatedEntries.forEach(entry -> tempMap.put(entry.getKey(), entry.getValue()));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        // renaming the mergedFile and update segment_0 file
        synchronized (keyDir) {
            try {
                readersLocks.acquireWriteLockForFile(BigInteger.ZERO);
                Files.delete(mergedFilePath);
                readersLocks.releaseWriteLockForFile(BigInteger.ZERO);

                Files.move(tempMergeFilePath, mergedFilePath, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

//            keyDir.entrySet()
//                    .stream()
//                    .parallel()
//                    .filter(entry -> entry.getValue().getFileId().compareTo(BigInteger.ZERO) == 0)
//                    .forEach(entry -> entry.setValue(tempMap.get(entry.getKey())));

            // merge the keyDir and tempMap
            tempMap.forEach((key, value) -> {
                KeyDirEntry keyDirEntry = keyDir.get(key);
                if (value.getTimeStamp().compareTo(keyDirEntry.getTimeStamp()) > 0)
                    keyDir.put(key, value);
            });
        }

//        // merge the keyDir and tempMap
//        tempMap.forEach((key, value) -> {
//            KeyDirEntry keyDirEntry = keyDir.get(key);
//            if (value.getTimeStamp().compareTo(keyDirEntry.getTimeStamp()) > 0)
//                keyDir.put(key, value);
//        });

        // delete oldFiles
        filesToDelete.forEach(segmentFile -> {
            try {
                readersLocks.acquireWriteLockForFile(segmentFile.getFileId());
                Files.delete(segmentFile.getFilePath());
                readersLocks.releaseWriteLockForFile(segmentFile.getFileId());
//                System.out.println("deleted file : " + segmentFile.getFileId());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        oldFilesCount.incrementAndGet();

        System.out.println("Done Merging " + Thread.currentThread() + "\n");
    }

    @Override
    public List<K> listKeys() {
        return keyDir
                .keySet()
                .stream()
                .toList();
    }

    @Override
    public <ACC> ACC fold(FoldFunction<K, V, ACC, ACC> F, ACC acc0) {
        ACC accumulator = acc0;
        for (var k : keyDir.keySet()) {
            Optional<V> value = get(k);
            if (value.isPresent())
                accumulator = F.apply(k, value.get(), accumulator);
        }
        return accumulator;
    }

    @Override
    public void sync() {
        try {
            activeFileChannel.force(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            sync();
            dirLocker.releaseWriteLock(options.getBaseDir());
            activeFileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
