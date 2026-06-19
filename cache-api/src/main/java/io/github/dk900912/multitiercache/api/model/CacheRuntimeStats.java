package io.github.dk900912.multitiercache.api.model;

/**
 * Snapshot of core runtime observability counters for the multi-tier cache framework.
 *
 * @author dukui
 */
public final class CacheRuntimeStats {

    private static final CacheRuntimeStats EMPTY = new CacheRuntimeStats(
            0L, 0L, 0L, 0L,
            0L, 0L,
            0L, 0L, 0L,
            0L, 0L, 0L, 0L,
            0L, 0L, 0L, 0L
    );

    private final long l1HitCount;
    private final long l1MissCount;
    private final long l2HitCount;
    private final long l2MissCount;
    private final long originLoadCount;
    private final long penetrationLoadCount;
    private final long l2ReadPathFailureCount;
    private final long l2MutationRejectedCount;
    private final long l2MutationFailureCount;
    private final long pubSubDroppedMessageCount;
    private final long pubSubInterruptionCount;
    private final long l1UntrustedBypassCount;
    private final long l1RecoveryClearFailureCount;
    private final long distributedLockAttemptCount;
    private final long distributedLockTimeoutCount;
    private final long distributedLockFailureCount;
    private final long distributedLockFailOpenLoadCount;

    public CacheRuntimeStats(long l1HitCount,
                             long l1MissCount,
                             long l2HitCount,
                             long l2MissCount,
                             long originLoadCount,
                             long penetrationLoadCount,
                             long l2ReadPathFailureCount,
                             long l2MutationRejectedCount,
                             long l2MutationFailureCount,
                             long pubSubDroppedMessageCount,
                             long pubSubInterruptionCount,
                             long l1UntrustedBypassCount,
                             long l1RecoveryClearFailureCount,
                             long distributedLockAttemptCount,
                             long distributedLockTimeoutCount,
                             long distributedLockFailureCount,
                             long distributedLockFailOpenLoadCount) {
        this.l1HitCount = l1HitCount;
        this.l1MissCount = l1MissCount;
        this.l2HitCount = l2HitCount;
        this.l2MissCount = l2MissCount;
        this.originLoadCount = originLoadCount;
        this.penetrationLoadCount = penetrationLoadCount;
        this.l2ReadPathFailureCount = l2ReadPathFailureCount;
        this.l2MutationRejectedCount = l2MutationRejectedCount;
        this.l2MutationFailureCount = l2MutationFailureCount;
        this.pubSubDroppedMessageCount = pubSubDroppedMessageCount;
        this.pubSubInterruptionCount = pubSubInterruptionCount;
        this.l1UntrustedBypassCount = l1UntrustedBypassCount;
        this.l1RecoveryClearFailureCount = l1RecoveryClearFailureCount;
        this.distributedLockAttemptCount = distributedLockAttemptCount;
        this.distributedLockTimeoutCount = distributedLockTimeoutCount;
        this.distributedLockFailureCount = distributedLockFailureCount;
        this.distributedLockFailOpenLoadCount = distributedLockFailOpenLoadCount;
    }

    public static CacheRuntimeStats empty() {
        return EMPTY;
    }

    public long getL1HitCount() {
        return l1HitCount;
    }

    public long getL1MissCount() {
        return l1MissCount;
    }

    public long getL2HitCount() {
        return l2HitCount;
    }

    public long getL2MissCount() {
        return l2MissCount;
    }

    public long getOriginLoadCount() {
        return originLoadCount;
    }

    public long getPenetrationLoadCount() {
        return penetrationLoadCount;
    }

    public long getL2ReadPathFailureCount() {
        return l2ReadPathFailureCount;
    }

    public long getL2MutationRejectedCount() {
        return l2MutationRejectedCount;
    }

    public long getL2MutationFailureCount() {
        return l2MutationFailureCount;
    }

    public long getPubSubDroppedMessageCount() {
        return pubSubDroppedMessageCount;
    }

    public long getPubSubInterruptionCount() {
        return pubSubInterruptionCount;
    }

    public long getL1UntrustedBypassCount() {
        return l1UntrustedBypassCount;
    }

    public long getL1RecoveryClearFailureCount() {
        return l1RecoveryClearFailureCount;
    }

    public long getDistributedLockAttemptCount() {
        return distributedLockAttemptCount;
    }

    public long getDistributedLockTimeoutCount() {
        return distributedLockTimeoutCount;
    }

    public long getDistributedLockFailureCount() {
        return distributedLockFailureCount;
    }

    public long getDistributedLockFailOpenLoadCount() {
        return distributedLockFailOpenLoadCount;
    }
}
