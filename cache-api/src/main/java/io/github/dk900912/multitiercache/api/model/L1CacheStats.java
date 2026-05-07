package io.github.dk900912.multitiercache.api.model;

/**
 * Represents the statistics for the Level 1 (L1) cache.
 *
 * @author dukui
 */
public class L1CacheStats {

    private long requestCount;
    private long hitCount;
    private double hitRate;
    private long missCount;
    private double missRate;
    private long loadCount;
    private long loadSuccessCount;
    private long loadFailureCount;
    private double loadFailureRate;
    private long totalLoadTime;
    private double averageLoadPenalty;
    private long evictionCount;

    public L1CacheStats() {}

    public long getRequestCount() {
        return requestCount;
    }

    public void setRequestCount(long requestCount) {
        this.requestCount = requestCount;
    }

    public long getHitCount() {
        return hitCount;
    }

    public void setHitCount(long hitCount) {
        this.hitCount = hitCount;
    }

    public double getHitRate() {
        return hitRate;
    }

    public void setHitRate(double hitRate) {
        this.hitRate = hitRate;
    }

    public long getMissCount() {
        return missCount;
    }

    public void setMissCount(long missCount) {
        this.missCount = missCount;
    }

    public double getMissRate() {
        return missRate;
    }

    public void setMissRate(double missRate) {
        this.missRate = missRate;
    }

    public long getLoadCount() {
        return loadCount;
    }

    public void setLoadCount(long loadCount) {
        this.loadCount = loadCount;
    }

    public long getLoadSuccessCount() {
        return loadSuccessCount;
    }

    public void setLoadSuccessCount(long loadSuccessCount) {
        this.loadSuccessCount = loadSuccessCount;
    }

    public long getLoadFailureCount() {
        return loadFailureCount;
    }

    public void setLoadFailureCount(long loadFailureCount) {
        this.loadFailureCount = loadFailureCount;
    }

    public double getLoadFailureRate() {
        return loadFailureRate;
    }

    public void setLoadFailureRate(double loadFailureRate) {
        this.loadFailureRate = loadFailureRate;
    }

    public long getTotalLoadTime() {
        return totalLoadTime;
    }

    public void setTotalLoadTime(long totalLoadTime) {
        this.totalLoadTime = totalLoadTime;
    }

    public double getAverageLoadPenalty() {
        return averageLoadPenalty;
    }

    public void setAverageLoadPenalty(double averageLoadPenalty) {
        this.averageLoadPenalty = averageLoadPenalty;
    }

    public long getEvictionCount() {
        return evictionCount;
    }

    public void setEvictionCount(long evictionCount) {
        this.evictionCount = evictionCount;
    }
}
