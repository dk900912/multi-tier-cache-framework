package io.github.dk900912.multitiercache.api.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CacheRuntimeStatsTest {

    @Test
    void emptyShouldExposeZeroForEveryCoreMetric() {
        assertStats(CacheRuntimeStats.empty(), 0L);
    }

    @Test
    void constructorShouldPreserveEveryCoreMetric() {
        CacheRuntimeStats stats = new CacheRuntimeStats(
                1L, 2L, 3L, 4L,
                5L, 6L,
                7L, 8L, 9L,
                10L, 11L, 12L, 13L,
                14L, 15L, 16L, 17L
        );

        assertAll(
                () -> assertEquals(1L, stats.getL1HitCount()),
                () -> assertEquals(2L, stats.getL1MissCount()),
                () -> assertEquals(3L, stats.getL2HitCount()),
                () -> assertEquals(4L, stats.getL2MissCount()),
                () -> assertEquals(5L, stats.getOriginLoadCount()),
                () -> assertEquals(6L, stats.getPenetrationLoadCount()),
                () -> assertEquals(7L, stats.getL2ReadPathFailureCount()),
                () -> assertEquals(8L, stats.getL2MutationRejectedCount()),
                () -> assertEquals(9L, stats.getL2MutationFailureCount()),
                () -> assertEquals(10L, stats.getPubSubDroppedMessageCount()),
                () -> assertEquals(11L, stats.getPubSubInterruptionCount()),
                () -> assertEquals(12L, stats.getL1UntrustedBypassCount()),
                () -> assertEquals(13L, stats.getL1RecoveryClearFailureCount()),
                () -> assertEquals(14L, stats.getDistributedLockAttemptCount()),
                () -> assertEquals(15L, stats.getDistributedLockTimeoutCount()),
                () -> assertEquals(16L, stats.getDistributedLockFailureCount()),
                () -> assertEquals(17L, stats.getDistributedLockFailOpenLoadCount())
        );
    }

    private static void assertStats(CacheRuntimeStats stats, long expected) {
        assertAll(
                () -> assertEquals(expected, stats.getL1HitCount()),
                () -> assertEquals(expected, stats.getL1MissCount()),
                () -> assertEquals(expected, stats.getL2HitCount()),
                () -> assertEquals(expected, stats.getL2MissCount()),
                () -> assertEquals(expected, stats.getOriginLoadCount()),
                () -> assertEquals(expected, stats.getPenetrationLoadCount()),
                () -> assertEquals(expected, stats.getL2ReadPathFailureCount()),
                () -> assertEquals(expected, stats.getL2MutationRejectedCount()),
                () -> assertEquals(expected, stats.getL2MutationFailureCount()),
                () -> assertEquals(expected, stats.getPubSubDroppedMessageCount()),
                () -> assertEquals(expected, stats.getPubSubInterruptionCount()),
                () -> assertEquals(expected, stats.getL1UntrustedBypassCount()),
                () -> assertEquals(expected, stats.getL1RecoveryClearFailureCount()),
                () -> assertEquals(expected, stats.getDistributedLockAttemptCount()),
                () -> assertEquals(expected, stats.getDistributedLockTimeoutCount()),
                () -> assertEquals(expected, stats.getDistributedLockFailureCount()),
                () -> assertEquals(expected, stats.getDistributedLockFailOpenLoadCount())
        );
    }
}
