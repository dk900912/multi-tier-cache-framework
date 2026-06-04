package io.github.dk900912.multitiercache.api.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CacheMessageTypeTest {

    @Test
    void testFromWireValue_ValidInsensitive() {
        assertEquals(CacheMessageType.INSERT, CacheMessageType.fromWireValue("insert"));
        assertEquals(CacheMessageType.INSERT, CacheMessageType.fromWireValue("INSERT"));
        assertEquals(CacheMessageType.UPDATE, CacheMessageType.fromWireValue("UpDaTe"));
        assertEquals(CacheMessageType.DELETE, CacheMessageType.fromWireValue("Delete"));
        assertEquals(CacheMessageType.PENETRATE, CacheMessageType.fromWireValue("penetrate"));
        assertEquals(CacheMessageType.BACKFILL, CacheMessageType.fromWireValue("backfill"));
    }

    @Test
    void testFromWireValue_Null() {
        assertThrows(IllegalArgumentException.class, () -> CacheMessageType.fromWireValue(null));
    }

    @Test
    void testFromWireValue_Invalid() {
        assertThrows(IllegalArgumentException.class, () -> CacheMessageType.fromWireValue("unknown"));
        assertThrows(IllegalArgumentException.class, () -> CacheMessageType.fromWireValue(""));
    }
}
