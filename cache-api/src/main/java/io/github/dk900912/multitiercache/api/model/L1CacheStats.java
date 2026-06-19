package io.github.dk900912.multitiercache.api.model;

/**
 * Represents the statistics for the Level 1 (L1) cache.
 *
 * @author dukui
 */
public class L1CacheStats {

    private long hitCount;
    private long missCount;
    private long evictionCount;

    public L1CacheStats() {}

    public long getHitCount() {
        return hitCount;
    }

    public void setHitCount(long hitCount) {
        this.hitCount = hitCount;
    }

    public long getMissCount() {
        return missCount;
    }

    public void setMissCount(long missCount) {
        this.missCount = missCount;
    }

    public long getEvictionCount() {
        return evictionCount;
    }

    public void setEvictionCount(long evictionCount) {
        this.evictionCount = evictionCount;
    }
}
