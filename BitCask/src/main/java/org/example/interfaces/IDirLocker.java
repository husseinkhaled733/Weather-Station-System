package org.example.interfaces;

public interface IDirLocker {
    /**
     * Acquires a write lock for the specified directory.
     *
     * @param directory The directory to lock.
     * @return True if the lock was successfully acquired, false otherwise.
     */
    boolean acquireWriteLock(String directory);

    /**
     * Releases the write lock for the specified directory.
     *
     * @param directory The directory to unlock.
     */
    void releaseWriteLock(String directory);
}
