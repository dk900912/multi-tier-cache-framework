package io.github.dk900912.multitiercache.api;

/**
 * Calculates fine-grained expiration times for cache entries based on their keys and values.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 * @author dukui
 */
public interface FineGrainedExpiry<K, V> {

    /**
     * Calculates the expiration duration after a cache entry is created.
     *
     * @param key   the cache key
     * @param value the cached value
     * @param currentTimeNanos the current time in nanoseconds
     * @return the duration in nanoseconds after which the entry should expire
     */
    long expireAfterCreate(K key, V value, long currentTimeNanos);

    /**
     * Calculates the expiration duration after a cache entry is updated.
     *
     * @param key   the cache key
     * @param value the cached value
     * @param currentTimeNanos the current time in nanoseconds
     * @param currentDurationNanos the current remaining duration in nanoseconds
     * @return the duration in nanoseconds after which the entry should expire
     */
    long expireAfterUpdate(K key, V value, long currentTimeNanos, long currentDurationNanos);

    /**
     * Calculates the expiration duration after a cache entry is accessed.
     *
     * @param key   the cache key
     * @param value the cached value
     * @param currentTimeNanos the current time in nanoseconds
     * @param currentDurationNanos the current remaining duration in nanoseconds
     * @return the duration in nanoseconds after which the entry should expire
     */
    long expireAfterRead(K key, V value, long currentTimeNanos, long currentDurationNanos);
}
