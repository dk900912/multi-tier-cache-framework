package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheMessageVersionComparatorTest {

    @Test
    void shouldRejectSameKeyReinsertAgainstDeleteTombstone() {
        CacheMessage<String> tombstone = new CacheMessage<>("user:1", null, 100L, CacheMessageType.DELETE, 1_000L);
        CacheMessage<String> reinsert = new CacheMessage<>("user:1", "alive", 101L, CacheMessageType.INSERT, 1_000L);

        assertFalse(CacheMessageVersionComparator.shouldReplace(reinsert, tombstone));
    }

    @Test
    void shouldRejectPenetrateAgainstRealValue() {
        CacheMessage<String> value = new CacheMessage<>("user:1", "alive", 10L, CacheMessageType.UPDATE, 1_000L);
        CacheMessage<String> penetrate = new CacheMessage<>("user:1", null, -1L, CacheMessageType.PENETRATE, 100L);

        assertFalse(CacheMessageVersionComparator.shouldReplace(penetrate, value));
    }

    @Test
    void shouldAllowDeleteWithEqualVersion() {
        CacheMessage<String> value = new CacheMessage<>("user:1", "alive", 8L, CacheMessageType.UPDATE, 1_000L);
        CacheMessage<String> delete = new CacheMessage<>("user:1", null, 8L, CacheMessageType.DELETE, 1_000L);

        assertTrue(CacheMessageVersionComparator.shouldReplace(delete, value));
    }
}
