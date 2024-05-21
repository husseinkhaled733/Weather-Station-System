package org.example;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReaderLocksHandler {

    private Map<BigInteger, ReentrantReadWriteLock> readersLocks;

    public ReaderLocksHandler() {
        this.readersLocks = new ConcurrentHashMap<>();
    }

    public void acquireReadLockForFile(BigInteger fileId) {
        readersLocks
                .computeIfAbsent(fileId, k -> new ReentrantReadWriteLock())
                .readLock()
                .lock();
    }

    public void releaseReadLockForFile(BigInteger fileId) {
        Optional.ofNullable(readersLocks.get(fileId))
                .ifPresent(lock -> lock.readLock().unlock());
    }

    public void acquireWriteLockForFile(BigInteger fileId) {
        readersLocks
                .computeIfAbsent(fileId, k -> new ReentrantReadWriteLock())
                .writeLock()
                .lock();
    }

    public void releaseWriteLockForFile(BigInteger fileId) {
        Optional.ofNullable(readersLocks.get(fileId))
                .ifPresent(lock -> lock
                        .writeLock()
                        .unlock());

    }
}
