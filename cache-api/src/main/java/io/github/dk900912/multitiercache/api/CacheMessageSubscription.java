package io.github.dk900912.multitiercache.api;

/**
 * Represents an active subscription to a cache message channel.
 * <p>
 * Implementations should close underlying connections when the subscription is no longer needed.
 * </p>
 *
 * @author dukui
 */
public interface CacheMessageSubscription extends AutoCloseable {
    @Override
    void close();
}
