package io.github.dk900912.multitiercache.provider.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.FineGrainedExpiry;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.L1CacheStats;
import io.github.dk900912.multitiercache.spi.L1Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Level 1 (L1) cache provider implementation based on Caffeine Cache.
 *
 * @author dukui
 */
public class CaffeineL1Provider implements L1Provider {

    private static final Logger LOGGER = LoggerFactory.getLogger(CaffeineL1Provider.class);

    private Cache<String, Object> cache;
    private boolean recordStats;

    public CaffeineL1Provider() {
    }

    public CaffeineL1Provider(Long maximumSize,
                              Duration expireAfterWrite,
                              Duration expireAfterAccess,
                              FineGrainedExpiry<String, Object> fineGrainedExpiry,
                              boolean recordStats) {
        initialize(maximumSize, expireAfterWrite, expireAfterAccess, fineGrainedExpiry, recordStats);
    }

    @Override
    public void initialize(CacheConfig.L1Config config) {
        initialize(
                config.getMaximumSize(),
                config.getExpireAfterWrite(),
                config.getExpireAfterAccess(),
                config.getFineGrainedExpiry(),
                config.isRecordStats()
        );
    }

    private void initialize(Long maximumSize,
                            Duration expireAfterWrite,
                            Duration expireAfterAccess,
                            FineGrainedExpiry<String, Object> fineGrainedExpiry,
                            boolean recordStats) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        if (maximumSize != null) {
            builder.maximumSize(maximumSize);
        }
        if (recordStats) {
            builder.recordStats();
        }
        if (fineGrainedExpiry != null) {
            if (expireAfterWrite != null || expireAfterAccess != null) {
                LOGGER.warn("FineGrainedExpiry is configured, the global expireAfterWrite/Access will be ignored.");
            }
            builder.expireAfter(adaptExpiry(fineGrainedExpiry));
        } else {
            if (expireAfterWrite != null) {
                builder.expireAfterWrite(expireAfterWrite);
            }
            if (expireAfterAccess != null) {
                builder.expireAfterAccess(expireAfterAccess);
            }
        }
        this.recordStats = recordStats;
        this.cache = builder.build();
    }

    private Expiry<String, Object> adaptExpiry(FineGrainedExpiry<String, Object> fineGrainedExpiry) {
        return new Expiry<>() {
            @Override
            public long expireAfterCreate(String key, Object value, long currentTime) {
                return fineGrainedExpiry.expireAfterCreate(key, value, currentTime);
            }

            @Override
            public long expireAfterUpdate(String key, Object value, long currentTime, long currentDuration) {
                return fineGrainedExpiry.expireAfterUpdate(key, value, currentTime, currentDuration);
            }

            @Override
            public long expireAfterRead(String key, Object value, long currentTime, long currentDuration) {
                return fineGrainedExpiry.expireAfterRead(key, value, currentTime, currentDuration);
            }
        };
    }

    @Override
    public Object get(CacheKey key) {
        ensureInitialized();
        return cache.getIfPresent(key.toRedisKey());
    }

    @Override
    public void put(CacheKey key, Object value) {
        ensureInitialized();
        cache.put(key.toRedisKey(), value);
    }

    @Override
    public void invalidate(CacheKey key) {
        ensureInitialized();
        cache.invalidate(key.toRedisKey());
    }

    @Override
    public void clear() {
        ensureInitialized();
        cache.invalidateAll();
    }

    @Override
    public L1CacheStats getStats() {
        ensureInitialized();
        if (!recordStats) {
            throw new IllegalStateException("Caffeine recordStats is disabled");
        }
        CacheStats stats = cache.stats();
        L1CacheStats snapshot = new L1CacheStats();
        snapshot.setRequestCount(stats.requestCount());
        snapshot.setHitCount(stats.hitCount());
        snapshot.setHitRate(stats.hitRate());
        snapshot.setMissCount(stats.missCount());
        snapshot.setMissRate(stats.missRate());
        snapshot.setLoadCount(stats.loadCount());
        snapshot.setLoadSuccessCount(stats.loadSuccessCount());
        snapshot.setLoadFailureCount(stats.loadFailureCount());
        snapshot.setLoadFailureRate(stats.loadFailureRate());
        snapshot.setTotalLoadTime(stats.totalLoadTime());
        snapshot.setAverageLoadPenalty(stats.averageLoadPenalty());
        snapshot.setEvictionCount(stats.evictionCount());
        return snapshot;
    }

    private void ensureInitialized() {
        if (cache == null) {
            throw new IllegalStateException("Caffeine L1 provider is not initialized");
        }
    }
}
