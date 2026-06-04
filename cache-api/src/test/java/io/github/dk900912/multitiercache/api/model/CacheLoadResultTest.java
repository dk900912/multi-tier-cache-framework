package io.github.dk900912.multitiercache.api.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheLoadResultTest {

    @Test
    void testOf_Success() {
        Duration ttl = Duration.ofMinutes(10);
        CacheLoadResult<String> result = CacheLoadResult.of("data", 1L, ttl);

        assertFalse(result.isPenetration());
        assertEquals("data", result.getData());
        assertEquals(1L, result.getVersion());
        assertEquals(ttl, result.getTtl());
    }

    @Test
    void testPenetration() {
        Duration ttl = Duration.ofMinutes(1);
        CacheLoadResult<String> result = CacheLoadResult.penetration(ttl);

        assertTrue(result.isPenetration());
        assertNull(result.getData());
        assertEquals(-1L, result.getVersion());
        assertEquals(ttl, result.getTtl());
    }

    @Test
    void testPenetration_NoTtl() {
        CacheLoadResult<String> result = CacheLoadResult.penetration(null);

        assertTrue(result.isPenetration());
        assertNull(result.getData());
        assertEquals(-1L, result.getVersion());
        assertNull(result.getTtl());
    }
}
