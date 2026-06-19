package io.github.dk900912.multitiercache.spi;

import java.time.Duration;

/**
 * Distributed reentrant lock abstraction provided by an L2 cache provider.
 * <p>
 * The lock is owned by the current Java thread. Implementations must reject
 * unlock attempts from threads that do not own the lock.
 * </p>
 *
 * @author dukui
 */
public interface L2ReentrantLock {

    /**
     * Acquires the lock, waiting until it becomes available.
     * <p>
     * Implementations should keep the lock alive with their watchdog mechanism
     * until it is fully unlocked by the owner.
     * </p>
     */
    void lock();

    /**
     * Acquires the lock, waiting until it becomes available, and lets it expire
     * automatically after the given lease time.
     *
     * @param leaseTime the lease time
     */
    void lock(Duration leaseTime);

    /**
     * Tries to acquire the lock immediately.
     *
     * @return {@code true} if the lock was acquired
     */
    boolean tryLock();

    /**
     * Tries to acquire the lock within the given wait time.
     *
     * @param waitTime the maximum wait time
     * @return {@code true} if the lock was acquired
     * @throws InterruptedException if interrupted while waiting
     */
    boolean tryLock(Duration waitTime) throws InterruptedException;

    /**
     * Tries to acquire the lock within the given wait time and lets it expire
     * automatically after the given lease time.
     *
     * @param waitTime  the maximum wait time
     * @param leaseTime the lease time
     * @return {@code true} if the lock was acquired
     * @throws InterruptedException if interrupted while waiting
     */
    boolean tryLock(Duration waitTime, Duration leaseTime) throws InterruptedException;

    /**
     * Releases one hold of the lock by the current thread.
     *
     * @throws IllegalMonitorStateException if the current thread does not own the lock
     */
    void unlock();

    /**
     * Releases the lock regardless of the current owner.
     *
     * @return {@code true} if a lock key was removed
     */
    boolean forceUnlock();

    /**
     * Returns whether the lock currently exists.
     *
     * @return {@code true} if the lock is held by any owner
     */
    boolean isLocked();

    /**
     * Returns whether the current thread owns the lock.
     *
     * @return {@code true} if the current thread owns the lock
     */
    boolean isHeldByCurrentThread();

    /**
     * Returns the current thread's reentrant hold count.
     *
     * @return the hold count, or 0 if the current thread does not own the lock
     */
    int getHoldCount();

    /**
     * Returns the remaining Redis key TTL.
     * <p>
     * Values follow Redis PTTL semantics: -2 ms means the lock key does not
     * exist, and -1 ms means it exists without expiration.
     * </p>
     *
     * @return the remaining TTL
     */
    Duration remainTimeToLive();
}
