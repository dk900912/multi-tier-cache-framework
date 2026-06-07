package io.github.dk900912.multitiercache.api;

import io.github.dk900912.multitiercache.api.model.L1CacheStats;
import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;

/**
 * Provides monitoring and statistics capabilities for the cache system.
 * <p>
 * This interface exposes runtime metrics that can be used to observe cache efficiency
 * and behavior, such as hit rates, eviction counts, and load penalties.
 * </p>
 *
 * @author dukui
 */
public interface CacheMonitor {

    /**
     * Retrieves the current statistics for the Level 1 (L1) cache.
     *
     * @return the {@link L1CacheStats} containing metrics like hits, misses, and evictions,
     *         or {@code null} if L1 cache monitoring is disabled or unavailable
     */
    L1CacheStats getL1CacheStats();

    /**
     * Retrieves runtime counters for core cache operations such as read paths,
     * L2 apply results, invalidation propagation, and compensation replay.
     *
     * @return an immutable snapshot of runtime counters
     */
    default CacheRuntimeStats getRuntimeStats() {
        return CacheRuntimeStats.empty();
    }
}
