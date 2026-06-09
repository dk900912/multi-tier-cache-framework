package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CacheManagerFactoryProviderSelectionTest {

    @Test
    void shouldCreateManagerWithExplicitJdkL1Provider() {
        CacheConfig config = l1OnlyConfig();
        config.getL1().setProvider(CacheConfig.L1ProviderType.JDK);

        CacheManager cacheManager = CacheManagerFactory.create(config);
        AtomicInteger loaderCalls = new AtomicInteger();
        CacheKey key = CacheKey.simple("factory:provider:jdk");

        String first = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("value", 1L, Duration.ofMinutes(1));
        });
        String second = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("unexpected", 2L, Duration.ofMinutes(1));
        });

        assertEquals("value", first);
        assertEquals("value", second);
        assertEquals(1, loaderCalls.get());
    }

    @Test
    void shouldFailWhenConfiguredL1ProviderWasNotLoaded() {
        CacheConfig config = l1OnlyConfig();
        config.getL1().setProvider(CacheConfig.L1ProviderType.CAFFEINE);

        assertThrows(IllegalStateException.class, () -> CacheManagerFactory.create(config));
    }

    private static CacheConfig l1OnlyConfig() {
        CacheConfig config = new CacheConfig();
        config.getL2().setEnabled(false);
        config.getCompensation().setEnabled(false);
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));
        return config;
    }
}
