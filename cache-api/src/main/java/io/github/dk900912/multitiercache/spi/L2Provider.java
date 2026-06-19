package io.github.dk900912.multitiercache.spi;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;

import java.time.Duration;
import java.util.List;

/**
 * Service Provider Interface (SPI) for Level 2 (L2) distributed cache.
 * <p>
 * Implementations typically wrap remote caches like Redis (e.g., using Lettuce, Redisson, or Jedis).
 * </p>
 *
 * @author dukui
 */
public interface L2Provider {

    /**
     * Initializes the L2 provider with the given configuration.
     *
     * @param config the L2 configuration
     */
    void initialize(CacheConfig.L2Config config);

    /**
     * Returns the built-in provider type represented by this implementation.
     * Custom implementations may keep AUTO and rely on classpath selection.
     */
    default CacheConfig.L2ProviderType providerType() {
        return CacheConfig.L2ProviderType.AUTO;
    }

    /**
     * Returns whether this provider implements the distributed lock contract.
     *
     * Providers returning {@code true} must honor the thread-ownership and watchdog
     * semantics documented by {@link L2ReentrantLock}.
     *
     * @return {@code true} when {@link #getLock(String)} is supported
     */
    default boolean supportsDistributedLock() {
        return false;
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * @param key the cache key
     * @return the cached value as string, or {@code null} if not found
     */
    String get(CacheKey key);

    /**
     * Associates the specified value with the given key, applying an optional TTL.
     *
     * @param key   the cache key
     * @param value the value to cache as a string
     * @param ttl   the positive time-to-live duration; must resolve to at least one millisecond
     */
    void set(CacheKey key, String value, Duration ttl);

    /**
     * Deletes the cached value for the specified key.
     *
     * @param key the cache key
     */
    void delete(CacheKey key);

    /**
     * Publishes a message using the requested Redis Pub/Sub routing mode.
     *
     * @param channel the Pub/Sub channel
     * @param message the message payload
     * @param mode    the Pub/Sub routing mode
     */
    void publish(String channel, String message, L2PubSubMode mode);

    /**
     * Subscribes using the requested Redis Pub/Sub routing mode.
     *
     * @param channel  the Pub/Sub channel
     * @param listener the listener to handle incoming messages
     * @param mode     the Pub/Sub routing mode
     * @return a subscription object to manage the connection
     */
    CacheMessageSubscription subscribe(
            String channel,
            CacheMessageListener listener,
            L2PubSubMode mode);

    /**
     * Evaluates a Lua script on the L2 cache server.
     *
     * @param script the Lua script
     * @param keys   the list of keys
     * @param args   the list of arguments
     * @return the result of the script evaluation
     */
    Object eval(String script, List<String> keys, List<String> args);

    /**
     * Returns a distributed reentrant lock backed by the L2 provider.
     *
     * @param name the lock name
     * @return a distributed reentrant lock handle
     */
    default L2ReentrantLock getLock(String name) {
        throw new UnsupportedOperationException("L2 distributed reentrant lock is not supported by this provider");
    }
}
