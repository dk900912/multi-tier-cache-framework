package io.github.dk900912.multitiercache.spi;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.L1CacheStats;

import java.util.function.BiFunction;

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
     * Returns the built-in provider type represented by this implementation.
     * Custom implementations may keep AUTO and rely on classpath selection.
     */
    default CacheConfig.L1ProviderType providerType() {
        return CacheConfig.L1ProviderType.AUTO;
    }

    /**
     * Whether this provider can expose native L1 cache statistics.
     */
    default boolean supportsRecordStats() {
        return false;
    }

    /**
     * Whether this provider can honor fine-grained per-entry expiry.
     */
    default boolean supportsFineGrainedExpiry() {
        return false;
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

    /**
     * Atomically computes a value for the specified key using the given remapping function.
     *
     * @param key               the cache key
     * @param remappingFunction the function to compute the new value
     * @return the new value associated with the key, or {@code null} if the entry was removed
     */
    default Object compute(CacheKey key, BiFunction<CacheKey, Object, Object> remappingFunction) {
        // Default implementation: non-atomic get-compute-put sequence
        // Implementations should override this with atomic compute operations for correctness
        Object oldValue = get(key);
        Object newValue = remappingFunction.apply(key, oldValue);
        if (newValue != null) {
            put(key, newValue);
        } else if (oldValue != null) {
            invalidate(key);
        }

        return newValue;
    }
}
