package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;

import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe recorder for core, actionable cache runtime metrics.
 *
 * @author dukui
 */
final class CacheRuntimeMetricsRecorder {

    private final LongAdder l1HitCount = new LongAdder();
    private final LongAdder l1MissCount = new LongAdder();
    private final LongAdder l2HitCount = new LongAdder();
    private final LongAdder l2MissCount = new LongAdder();
    private final LongAdder originLoadCount = new LongAdder();
    private final LongAdder penetrationLoadCount = new LongAdder();
    private final LongAdder l2ReadPathFailureCount = new LongAdder();
    private final LongAdder l2MutationRejectedCount = new LongAdder();
    private final LongAdder l2MutationFailureCount = new LongAdder();
    private final LongAdder pubSubDroppedMessageCount = new LongAdder();
    private final LongAdder pubSubInterruptionCount = new LongAdder();
    private final LongAdder l1UntrustedBypassCount = new LongAdder();
    private final LongAdder l1RecoveryClearFailureCount = new LongAdder();
    private final LongAdder distributedLockAttemptCount = new LongAdder();
    private final LongAdder distributedLockTimeoutCount = new LongAdder();
    private final LongAdder distributedLockFailureCount = new LongAdder();
    private final LongAdder distributedLockFailOpenLoadCount = new LongAdder();

    void recordL1HitCount() {
        l1HitCount.increment();
    }

    void recordL1MissCount() {
        l1MissCount.increment();
    }

    void recordL2HitCount() {
        l2HitCount.increment();
    }

    void recordL2MissCount() {
        l2MissCount.increment();
    }

    void recordOriginLoadCount(boolean penetration) {
        originLoadCount.increment();
        if (penetration) {
            penetrationLoadCount.increment();
        }
    }

    void recordL2ReadPathFailureCount() {
        l2ReadPathFailureCount.increment();
    }

    void recordL2MutationRejectedCount() {
        l2MutationRejectedCount.increment();
    }

    void recordL2MutationFailureCount() {
        l2MutationFailureCount.increment();
    }

    void recordPubSubDroppedMessageCount(long count) {
        if (count > 0) {
            pubSubDroppedMessageCount.add(count);
        }
    }

    void recordPubSubInterruptionCount() {
        pubSubInterruptionCount.increment();
    }

    void recordL1UntrustedBypassCount() {
        l1UntrustedBypassCount.increment();
    }

    void recordL1RecoveryClearFailureCount() {
        l1RecoveryClearFailureCount.increment();
    }

    void recordDistributedLockAttemptCount() {
        distributedLockAttemptCount.increment();
    }

    void recordDistributedLockTimeoutCount() {
        distributedLockTimeoutCount.increment();
    }

    void recordDistributedLockFailureCount() {
        distributedLockFailureCount.increment();
    }

    void recordDistributedLockFailOpenLoadCount() {
        distributedLockFailOpenLoadCount.increment();
    }

    CacheRuntimeStats snapshot() {
        return new CacheRuntimeStats(
                l1HitCount.sum(),
                l1MissCount.sum(),
                l2HitCount.sum(),
                l2MissCount.sum(),
                originLoadCount.sum(),
                penetrationLoadCount.sum(),
                l2ReadPathFailureCount.sum(),
                l2MutationRejectedCount.sum(),
                l2MutationFailureCount.sum(),
                pubSubDroppedMessageCount.sum(),
                pubSubInterruptionCount.sum(),
                l1UntrustedBypassCount.sum(),
                l1RecoveryClearFailureCount.sum(),
                distributedLockAttemptCount.sum(),
                distributedLockTimeoutCount.sum(),
                distributedLockFailureCount.sum(),
                distributedLockFailOpenLoadCount.sum()
        );
    }
}
