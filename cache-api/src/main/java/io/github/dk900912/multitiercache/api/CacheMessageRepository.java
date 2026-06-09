package io.github.dk900912.multitiercache.api;

import io.github.dk900912.multitiercache.api.model.CacheMessage;

import java.util.List;

/**
 * Service Provider Interface (SPI) for storing and managing cache messages locally.
 * <p>
 * Used primarily for compensating failed writes from the current node to L2.
 * It does not make Redis Pub/Sub itself reliable and cannot recover a peer node
 * that missed an already-published invalidation message.
 * </p>
 *
 * @author dukui
 */
public interface CacheMessageRepository {

    /**
     * Saves a cache message for later compensation.
     *
     * @param message the cache message to save
     */
    void save(CacheMessage<?> message);

    /**
     * Fetches a batch of unprocessed cache messages.
     *
     * @param limit the maximum number of messages to fetch
     * @return a non-null list of unprocessed messages whose elements must also be non-null
     */
    List<CacheMessage<?>> fetchUnprocessed(int limit);

    /**
     * Marks a specific cache message as processed.
     *
     * @param key        the cache key
     * @param version the version of the message
     */
    void markProcessed(String key, Long version);
}
