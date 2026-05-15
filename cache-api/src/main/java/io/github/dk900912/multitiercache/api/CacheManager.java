package io.github.dk900912.multitiercache.api;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * The central interface for interacting with the multi-tier cache framework.
 * <p>
 * <code>CacheManager</code> provides high-level operations for retrieving, inserting,
 * updating, and evicting cache entries. It abstracts away the complexity of L1 and L2
 * coordination, cache miss handling, and cache breakdown protection (e.g., SingleFlight).
 * </p>
 *
 * @author dukui
 */
public interface CacheManager extends CacheMutationProcessor, LifecycleManager {

    /**
     * Retrieves a value from the cache by its key, or loads it using the provided {@link Supplier} if absent.
     * Uses the default Time-To-Live (TTL) defined in the configuration.
     *
     * @param key    the cache key
     * @param loader a supplier to load the value if the cache misses; returning {@code null} is treated as cache penetration
     * @param <T>    the type of the cached value
     * @return the cached or newly loaded value
     */
    <T> T get(CacheKey key, Supplier<T> loader);

    /**
     * Retrieves a value from the cache by its key, or loads it using the provided {@link Supplier} if absent,
     * specifying a custom TTL for the loaded value.
     *
     * @param key    the cache key
     * @param loader a supplier to load the value if the cache misses; returning {@code null} is treated as cache penetration
     * @param ttl    the time-to-live duration applied to the L2 cache (L1 expiry is governed by local configuration)
     * @param <T>    the type of the cached value
     * @return the cached or newly loaded value
     */
    <T> T get(CacheKey key, Supplier<T> loader, Duration ttl);

    /**
     * Retrieves a value from the cache using a sophisticated {@link CacheLoader} that provides
     * detailed load results including version and TTL.
     *
     * @param key    the cache key
     * @param loader the cache loader providing the value, version, and TTL
     * @param <T>    the type of the cached value
     * @return the cached or newly loaded value
     */
    <T> T get(CacheKey key, CacheLoader<T> loader);

    /**
     * Inserts a new value into the cache, propagating the change to L2 and invalidating L1.
     *
     * @param key     the cache key
     * @param data    the payload data to cache
     * @param version the version of the data (used for concurrency control and conflict resolution)
     * @param ttl     the time-to-live duration applied to the L2 cache (L1 expiry is governed by local configuration)
     */
    void insert(CacheKey key, Object data, Long version, Duration ttl);

    /**
     * Updates an existing value in the cache, propagating the change to L2 and invalidating L1.
     *
     * @param key     the cache key
     * @param data    the new payload data
     * @param version the new version of the data
     * @param ttl     the time-to-live duration applied to the L2 cache (L1 expiry is governed by local configuration)
     */
    void update(CacheKey key, Object data, Long version, Duration ttl);

    /**
     * Evicts a value from the cache, propagating the deletion to L2 and invalidating L1.
     *
     * @param key     the cache key
     * @param version the version of the deletion operation
     * @param ttl     the time-to-live duration applied to the L2 cache tombstone/deletion marker
     */
    void evict(CacheKey key, Long version, Duration ttl);

    /**
     * Retrieves the monitor to observe cache statistics and metrics.
     *
     * @return the {@link CacheMonitor} instance
     */
    CacheMonitor getMonitor();
}
