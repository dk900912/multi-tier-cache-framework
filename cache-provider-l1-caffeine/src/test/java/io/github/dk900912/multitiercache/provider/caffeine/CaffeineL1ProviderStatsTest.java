package io.github.dk900912.multitiercache.provider.caffeine;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.model.L1CacheStats;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CaffeineL1ProviderStatsTest {

    @Test
    void shouldExposeOnlyUsefulNativeStats() {
        CaffeineL1Provider provider = new CaffeineL1Provider(null, null, null, null, true);
        CacheKey key = CacheKey.simple("stats:caffeine:1");

        provider.get(key);
        provider.put(key, "value");
        provider.get(key);

        L1CacheStats stats = provider.getStats();

        assertEquals(1L, stats.getHitCount());
        assertEquals(1L, stats.getMissCount());
        assertEquals(Set.of("getHitCount", "getMissCount", "getEvictionCount"), getterNames());
    }

    private static Set<String> getterNames() {
        return Arrays.stream(L1CacheStats.class.getDeclaredMethods())
                .filter(method -> method.getName().startsWith("get"))
                .map(Method::getName)
                .collect(Collectors.toSet());
    }
}
