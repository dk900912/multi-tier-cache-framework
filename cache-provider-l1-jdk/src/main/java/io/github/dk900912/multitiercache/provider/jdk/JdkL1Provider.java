package io.github.dk900912.multitiercache.provider.jdk;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.L1CacheStats;
import io.github.dk900912.multitiercache.spi.L1Provider;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;


/**
 * Level 1 (L1) cache provider implementation based on JDK's {@link java.util.LinkedHashMap}.
 *
 * @author dukui
 */
public class JdkL1Provider implements L1Provider {

    private Map<String, CacheEntry> cache;
    private Long maximumSize;
    private Duration expireAfterWrite;
    private Duration expireAfterAccess;
    private final ReentrantLock lock = new ReentrantLock();

    public JdkL1Provider() {
    }

    public JdkL1Provider(Long maximumSize, Duration expireAfterWrite, Duration expireAfterAccess) {
        initialize(maximumSize, expireAfterWrite, expireAfterAccess);
    }

    @Override
    public void initialize(CacheConfig.L1Config config) {
        initialize(config.getMaximumSize(), config.getExpireAfterWrite(), config.getExpireAfterAccess());
    }

    private void initialize(Long maximumSize, Duration expireAfterWrite, Duration expireAfterAccess) {
        this.maximumSize = maximumSize;
        this.expireAfterWrite = expireAfterWrite;
        this.expireAfterAccess = expireAfterAccess;
        this.cache = new LinkedHashMap<String, CacheEntry>(16, 0.75F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
                return JdkL1Provider.this.maximumSize != null
                        && JdkL1Provider.this.maximumSize > 0 
                        && size() > JdkL1Provider.this.maximumSize;
            }
        };
    }

    @Override
    public Object get(CacheKey key) {
        ensureInitialized();
        lock.lock();
        try {
            String keyString = key.toKeyString();
            CacheEntry entry = cache.get(keyString);
            if (entry == null) {
                return null;
            }

            long now = System.currentTimeMillis();
            if (isExpired(entry, now)) {
                cache.remove(keyString);
                return null;
            }

            entry.accessTime = now;
            return entry.value;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(CacheKey key, Object value) {
        ensureInitialized();
        lock.lock();
        try {
            String keyString = key.toKeyString();
            long now = System.currentTimeMillis();
            cache.put(keyString, new CacheEntry(value, now, now));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void invalidate(CacheKey key) {
        ensureInitialized();
        lock.lock();
        try {
            cache.remove(key.toKeyString());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Object compute(CacheKey key, BiFunction<CacheKey, Object, Object> remappingFunction) {
        ensureInitialized();
        lock.lock();
        try {
            String keyString = key.toKeyString();
            long now = System.currentTimeMillis();
            
            // Get current value (checking for expiration)
            CacheEntry entry = cache.get(keyString);
            Object currentValue = null;
            if (entry != null) {
                if (isExpired(entry, now)) {
                    cache.remove(keyString);
                } else {
                    currentValue = entry.value;
                }
            }
            
            // Compute new value
            Object newValue = remappingFunction.apply(key, currentValue);
            
            // Update cache based on new value
            if (newValue != null) {
                cache.put(keyString, new CacheEntry(newValue, now, now));
            } else if (entry != null) {
                cache.remove(keyString);
            }
            
            return newValue;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        ensureInitialized();
        lock.lock();
        try {
            cache.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public L1CacheStats getStats() {
        throw new UnsupportedOperationException("JDK L1 provider does not support recordStats");
    }

    private boolean isExpired(CacheEntry entry, long now) {
        if (expireAfterWrite != null && now - entry.writeTime > expireAfterWrite.toMillis()) {
            return true;
        }
        return expireAfterAccess != null && now - entry.accessTime > expireAfterAccess.toMillis();
    }

    private void ensureInitialized() {
        if (cache == null) {
            throw new IllegalStateException("JDK L1 provider is not initialized");
        }
    }

    private static class CacheEntry {
        final Object value;
        final long writeTime;
        long accessTime;

        CacheEntry(Object value, long writeTime, long accessTime) {
            this.value = value;
            this.writeTime = writeTime;
            this.accessTime = accessTime;
        }
    }
}
