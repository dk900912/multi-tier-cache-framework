package io.github.dk900912.multitiercache.provider.guava;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class GuavaL1ProviderTest {

    private GuavaL1Provider provider;

    @BeforeEach
    void setUp() {
        provider = new GuavaL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        config.setExpireAfterWrite(Duration.ofSeconds(1));
        provider.initialize(config);
    }

    @Test
    void testBasicPutAndGet() {
        CacheKey key = CacheKey.simple("test-key");
        provider.put(key, "test-value");
        assertEquals("test-value", provider.get(key));
    }

    @Test
    void testInvalidate() {
        CacheKey key = CacheKey.simple("test-key-2");
        provider.put(key, "test-value-2");
        provider.invalidate(key);
        assertNull(provider.get(key));
    }

    @Test
    void testClear() {
        provider.put(CacheKey.simple("k1"), "v1");
        provider.put(CacheKey.simple("k2"), "v2");
        provider.clear();
        assertNull(provider.get(CacheKey.simple("k1")));
        assertNull(provider.get(CacheKey.simple("k2")));
    }
}
