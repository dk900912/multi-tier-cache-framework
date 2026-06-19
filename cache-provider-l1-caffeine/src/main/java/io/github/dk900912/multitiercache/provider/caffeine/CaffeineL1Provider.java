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
import java.util.function.BiFunction;

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

    @Override
    public CacheConfig.L1ProviderType providerType() {
        return CacheConfig.L1ProviderType.CAFFEINE;
    }

    @Override
    public boolean supportsRecordStats() {
        return true;
    }

    @Override
    public boolean supportsFineGrainedExpiry() {
        return true;
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
            if (expireAfterAccess != null) {
                LOGGER.warn("FineGrainedExpiry is configured; global expireAfterAccess is ignored.");
            }
            builder.expireAfter(adaptExpiry(fineGrainedExpiry, expireAfterWrite));
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

    private Expiry<String, Object> adaptExpiry(
            FineGrainedExpiry<String, Object> fineGrainedExpiry,
            Duration expireAfterWrite) {
        long writeExpiryCapNanos = toSaturatedNanos(expireAfterWrite);
        return new Expiry<>() {
            @Override
            public long expireAfterCreate(String key, Object value, long currentTime) {
                return Math.min(
                        fineGrainedExpiry.expireAfterCreate(key, value, currentTime),
                        writeExpiryCapNanos);
            }

            @Override
            public long expireAfterUpdate(String key, Object value, long currentTime, long currentDuration) {
                return Math.min(
                        fineGrainedExpiry.expireAfterUpdate(key, value, currentTime, currentDuration),
                        writeExpiryCapNanos);
            }

            @Override
            public long expireAfterRead(String key, Object value, long currentTime, long currentDuration) {
                // Never allow a read to extend the deadline established by create/update.
                return Math.min(
                        fineGrainedExpiry.expireAfterRead(key, value, currentTime, currentDuration),
                        currentDuration);
            }
        };
    }

    private static long toSaturatedNanos(Duration duration) {
        if (duration == null) {
            return Long.MAX_VALUE;
        }
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
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
            throw new IllegalStateException("Caffeine recordStats is disabled");
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
            throw new IllegalStateException("Caffeine L1 provider is not initialized");
        }
    }
}
