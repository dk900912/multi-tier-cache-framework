package io.github.dk900912.multitiercache.spi;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.L1CacheStats;

/**
 * Service Provider Interface (SPI) for Level 1 (L1) local cache.
 * <p>
 * Implementations typically wrap local in-memory caches like Caffeine, Guava, or JDK Map.
 * </p>
 *
 * @author dukui
 */
public interface L1Provider {

    /**
     * Initializes the L1 provider with the given configuration.
     *
     * @param config the L1 configuration
     */
    default void initialize(CacheConfig.L1Config config) {
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * @param key the cache key
     * @return the cached value, or {@code null} if not found
     */
    Object get(CacheKey key);

    /**
     * Associates the specified value with the given key.
     *
     * @param key   the cache key
     * @param value the value to cache
     */
    void put(CacheKey key, Object value);

    /**
     * Invalidates (removes) the cached value for the specified key.
     *
     * @param key the cache key
     */
    void invalidate(CacheKey key);

    /**
     * Clears all entries from the L1 cache.
     */
    void clear();

    /**
     * Retrieves the current statistics for the L1 cache.
     *
     * @return the {@link L1CacheStats}, or {@code null} if stats are not recorded
     */
    default L1CacheStats getStats() {
        return null;
    }
}
