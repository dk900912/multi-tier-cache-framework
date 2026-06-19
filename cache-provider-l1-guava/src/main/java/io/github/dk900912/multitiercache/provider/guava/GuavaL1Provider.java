package io.github.dk900912.multitiercache.provider.guava;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.L1CacheStats;
import io.github.dk900912.multitiercache.spi.L1Provider;

import java.time.Duration;
import java.util.function.BiFunction;

/**
 * Level 1 (L1) cache provider implementation based on Google Guava Cache.
 *
 * @author dukui
 */
public class GuavaL1Provider implements L1Provider {

    private Cache<String, Object> cache;
    private boolean recordStats;

    public GuavaL1Provider() {
    }

    @Override
    public CacheConfig.L1ProviderType providerType() {
        return CacheConfig.L1ProviderType.GUAVA;
    }

    @Override
    public boolean supportsRecordStats() {
        return true;
    }

    public GuavaL1Provider(Long maximumSize,
                           Duration expireAfterWrite,
                           Duration expireAfterAccess,
                           boolean recordStats) {
        initialize(maximumSize, expireAfterWrite, expireAfterAccess, recordStats);
    }

    @Override
    public void initialize(CacheConfig.L1Config config) {
        initialize(
                config.getMaximumSize(),
                config.getExpireAfterWrite(),
                config.getExpireAfterAccess(),
                config.isRecordStats()
        );
    }

    private void initialize(Long maximumSize,
                            Duration expireAfterWrite,
                            Duration expireAfterAccess,
                            boolean recordStats) {
        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
        if (maximumSize != null) {
            builder.maximumSize(maximumSize);
        }
        if (expireAfterWrite != null) {
            builder.expireAfterWrite(expireAfterWrite);
        }
        if (expireAfterAccess != null) {
            builder.expireAfterAccess(expireAfterAccess);
        }
        if (recordStats) {
            builder.recordStats();
        }
        this.recordStats = recordStats;
        this.cache = builder.build();
    }

    @Override
    public Object get(CacheKey key) {
        ensureInitialized();
        return cache.getIfPresent(key.toKeyString());
    }

    @Override
    public void put(CacheKey key, Object value) {
        ensureInitialized();
        cache.put(key.toKeyString(), value);
    }

    @Override
    public void invalidate(CacheKey key) {
        ensureInitialized();
        cache.invalidate(key.toKeyString());
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
            throw new IllegalStateException("Guava recordStats is disabled");
        }
        CacheStats stats = cache.stats();
        L1CacheStats snapshot = new L1CacheStats();
        snapshot.setHitCount(stats.hitCount());
        snapshot.setMissCount(stats.missCount());
        snapshot.setEvictionCount(stats.evictionCount());
        return snapshot;
    }

    @Override
    public Object compute(CacheKey key, BiFunction<CacheKey, Object, Object> remappingFunction) {
        ensureInitialized();
        return cache.asMap().compute(key.toKeyString(), (k, v) -> remappingFunction.apply(key, v));
    }

    private void ensureInitialized() {
        if (cache == null) {
            throw new IllegalStateException("Guava L1 provider is not initialized");
        }
    }
}
