package io.github.dk900912.multitiercache.provider.jdk;

import io.github.dk900912.multitiercache.api.CacheKey;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class JdkL1ProviderTest {

    @Test
    void shouldNeverExceedMaximumSize() throws Exception {
        JdkL1Provider provider = new JdkL1Provider(2L, null, null);

        provider.put(CacheKey.simple("k1"), "v1");
        provider.put(CacheKey.simple("k2"), "v2");
        provider.put(CacheKey.simple("k3"), "v3");

        assertEquals(2, internalSize(provider));
        assertNull(provider.get(CacheKey.simple("k1")));
        assertEquals("v2", provider.get(CacheKey.simple("k2")));
        assertEquals("v3", provider.get(CacheKey.simple("k3")));
    }

    @Test
    void shouldKeepRecentlyAccessedEntryWhenEvicting() throws Exception {
        JdkL1Provider provider = new JdkL1Provider(2L, null, null);

        provider.put(CacheKey.simple("k1"), "v1");
        provider.put(CacheKey.simple("k2"), "v2");
        assertEquals("v1", provider.get(CacheKey.simple("k1")));

        provider.put(CacheKey.simple("k3"), "v3");

        assertEquals(2, internalSize(provider));
        assertEquals("v1", provider.get(CacheKey.simple("k1")));
        assertNull(provider.get(CacheKey.simple("k2")));
        assertEquals("v3", provider.get(CacheKey.simple("k3")));
    }

    @Test
    void shouldRemoveExpiredEntryBeforeReturningValue() throws Exception {
        JdkL1Provider provider = new JdkL1Provider(2L, Duration.ofMillis(5), null);

        provider.put(CacheKey.simple("k1"), "v1");
        Thread.sleep(20L);

        assertNull(provider.get(CacheKey.simple("k1")));
        assertEquals(0, internalSize(provider));
    }

    @SuppressWarnings("unchecked")
    private static int internalSize(JdkL1Provider provider) throws Exception {
        Field field = JdkL1Provider.class.getDeclaredField("cache");
        field.setAccessible(true);
        Map<String, ?> cache = (Map<String, ?>) field.get(provider);
        assertNotNull(cache);
        return cache.size();
    }
}
