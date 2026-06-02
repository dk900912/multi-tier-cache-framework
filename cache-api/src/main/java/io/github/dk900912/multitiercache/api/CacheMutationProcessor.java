package io.github.dk900912.multitiercache.api;

import io.github.dk900912.multitiercache.api.model.CacheMessage;

/**
 * Processor for handling cache mutation messages.
 * <p>
 * Responsible for applying mutations (e.g., from L2 Pub/Sub or compensation replayer) to the local L1 cache.
 * </p>
 *
 * @author dukui
 */
public interface CacheMutationProcessor {

    /**
     * Applies a cache mutation message.
     *
     * @param message the cache message detailing the mutation
     */
    void apply(CacheMessage<?> message);
}
