package io.github.dk900912.multitiercache.provider.caffeine;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.FineGrainedExpiry;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CaffeineL1ProviderTest {

    @Test
    void testBasicPutAndGet() {
        CaffeineL1Provider provider = new CaffeineL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        config.setExpireAfterWrite(Duration.ofMinutes(1));
        provider.initialize(config);

        CacheKey key = CacheKey.simple("caffeine-key");
        provider.put(key, "caffeine-value");
        assertEquals("caffeine-value", provider.get(key));
        
        provider.invalidate(key);
        assertNull(provider.get(key));
    }

    @Test
    void testFineGrainedExpiry() throws InterruptedException {
        CaffeineL1Provider provider = new CaffeineL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        
        // Define fine-grained expiry: key "short" expires in 10ms, key "long" in 1 day
        config.setFineGrainedExpiry(new FineGrainedExpiry<String, Object>() {
            @Override
            public long expireAfterCreate(String key, Object value, long currentTime) {
                if ("short".equals(key)) {
                    return Duration.ofMillis(10).toNanos();
                }
                return Duration.ofDays(1).toNanos();
            }

            @Override
            public long expireAfterUpdate(String key, Object value, long currentTime, long currentDuration) {
                return currentDuration;
            }

            @Override
            public long expireAfterRead(String key, Object value, long currentTime, long currentDuration) {
                return currentDuration;
            }
        });
        
        provider.initialize(config);

        CacheKey shortKey = CacheKey.simple("short");
        CacheKey longKey = CacheKey.simple("long");
        
        provider.put(shortKey, "v1");
        provider.put(longKey, "v2");
        
        // Give it a tiny bit of time to expire the short key
        Thread.sleep(50);
        
        assertNull(provider.get(shortKey), "Short key should have expired");
        assertEquals("v2", provider.get(longKey), "Long key should still be present");
    }
}
