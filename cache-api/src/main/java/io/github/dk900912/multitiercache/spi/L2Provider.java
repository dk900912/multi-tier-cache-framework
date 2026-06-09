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
    default void initialize(CacheConfig.L2Config config) {
    }

    /**
     * Returns the built-in provider type represented by this implementation.
     * Custom implementations may keep AUTO and rely on classpath selection.
     */
    default CacheConfig.L2ProviderType providerType() {
        return CacheConfig.L2ProviderType.AUTO;
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
     * @param ttl   the time-to-live duration, or {@code null} for no expiration
     */
    void set(CacheKey key, String value, Duration ttl);

    /**
     * Deletes the cached value for the specified key.
     *
     * @param key the cache key
     */
    void delete(CacheKey key);

    /**
     * Publishes a message to the specified channel.
     *
     * @param channel the Pub/Sub channel
     * @param message the message payload
     */
    void publish(String channel, String message);

    /**
     * Subscribes to a channel to receive messages.
     *
     * @param channel  the Pub/Sub channel
     * @param listener the listener to handle incoming messages
     * @return a subscription object to manage the connection
     */
    CacheMessageSubscription subscribe(String channel, CacheMessageListener listener);

    /**
     * Evaluates a Lua script on the L2 cache server.
     *
     * @param script the Lua script
     * @param keys   the list of keys
     * @param args   the list of arguments
     * @return the result of the script evaluation
     */
    Object eval(String script, List<String> keys, List<String> args);
}
