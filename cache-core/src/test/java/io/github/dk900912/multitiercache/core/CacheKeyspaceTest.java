package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheKeyspaceTest {

    @Test
    void shouldKeepDataAndGenerationKeysStableAndInSameSlot() {
        String businessKey = "user:3";

        String dataKey = CacheKeyspace.dataKey(businessKey);
        String generationKey = CacheKeyspace.generationKey(businessKey);
        String identifier = CacheKeyspace.identifier(businessKey);

        assertEquals("mtc:data:{" + identifier + "}", dataKey);
        assertEquals("mtc:gen:{" + identifier + "}", generationKey);
        assertEquals(extractSlotTag(dataKey), extractSlotTag(generationKey));
        assertEquals(dataKey, CacheKeyspace.dataKey(CacheKey.simple(businessKey)).toKeyString());
        assertEquals(generationKey, CacheKeyspace.generationKey(CacheKey.simple(businessKey)).toKeyString());
    }

    @Test
    void shouldNotLeakRawBusinessKeyIntoRedisKeys() {
        String businessKey = "tenant-a:user:{sensitive}:name=Sun Xiaoqin\nmobile=13800000000";

        String dataKey = CacheKeyspace.dataKey(businessKey);
        String generationKey = CacheKeyspace.generationKey(businessKey);

        assertFalse(dataKey.contains(businessKey));
        assertFalse(generationKey.contains(businessKey));
        assertFalse(dataKey.contains("\n"));
        assertFalse(generationKey.contains("\n"));
        assertTrue(dataKey.matches("^mtc:data:\\{[A-Za-z0-9_-]{43}}$"));
        assertTrue(generationKey.matches("^mtc:gen:\\{[A-Za-z0-9_-]{43}}$"));
    }

    @Test
    void shouldUseCompactBase64UrlIdentifierInsteadOfHex() {
        String identifier = CacheKeyspace.identifier("user:compact:test");

        assertEquals(43, identifier.length());
        assertTrue(identifier.matches("^[A-Za-z0-9_-]{43}$"));
    }

    @Test
    void shouldProduceDifferentOpaqueIdentifiersForDifferentBusinessKeys() {
        String first = CacheKeyspace.dataKey("user:1");
        String second = CacheKeyspace.dataKey("user:2");

        assertNotEquals(first, second);
        assertNotEquals(CacheKeyspace.identifier("user:1"), CacheKeyspace.identifier("user:2"));
    }

    private static String extractSlotTag(String key) {
        int start = key.indexOf('{');
        int end = key.indexOf('}');
        return key.substring(start + 1, end);
    }
}
