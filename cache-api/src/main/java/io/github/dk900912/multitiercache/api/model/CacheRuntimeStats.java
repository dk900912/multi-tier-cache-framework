package io.github.dk900912.multitiercache.api.model;

/**
 * Snapshot of runtime observability counters for the multi-tier cache framework.
 *
 * @author dukui
 */
public final class CacheRuntimeStats {

    private static final CacheRuntimeStats EMPTY = new CacheRuntimeStats(
            0L, 0L, 0L, 0L,
            0L, 0L, 0L,
            0L, 0L,
            0L, 0L,
            0L, 0L, 0L,
            0L, 0L, 0L,
            0L, 0L,
            0L,
            0L, 0L, 0L, 0L, 0L
    );

    private final long l1Hits;
    private final long l1Misses;
    private final long l2Hits;
    private final long l2Misses;
    private final long loaderCalls;
    private final long loaderValueCalls;
    private final long loaderPenetrationCalls;
    private final long l1BackfillApplied;
    private final long l1BackfillSkipped;
    private final long l1InvalidationsApplied;
    private final long l1InvalidationsSkipped;
    private final long l2ReadApplyAccepted;
    private final long l2ReadApplyRejected;
    private final long l2ReadApplyFailures;
    private final long l2MutationApplyAccepted;
    private final long l2MutationApplyRejected;
    private final long l2MutationApplyFailures;
    private final long compensationSaveSuccesses;
    private final long compensationSaveFailures;
    private final long pubSubMessagesReceived;
    private final long replayRuns;
    private final long replayMessagesFetched;
    private final long replayMessagesSkipped;
    private final long replayMessagesApplied;
    private final long replayMessagesFailed;

    public CacheRuntimeStats(long l1Hits,
                             long l1Misses,
                             long l2Hits,
                             long l2Misses,
                             long loaderCalls,
                             long loaderValueCalls,
                             long loaderPenetrationCalls,
                             long l1BackfillApplied,
                             long l1BackfillSkipped,
                             long l1InvalidationsApplied,
                             long l1InvalidationsSkipped,
                             long l2ReadApplyAccepted,
                             long l2ReadApplyRejected,
                             long l2ReadApplyFailures,
                             long l2MutationApplyAccepted,
                             long l2MutationApplyRejected,
                             long l2MutationApplyFailures,
                             long compensationSaveSuccesses,
                             long compensationSaveFailures,
                             long pubSubMessagesReceived,
                             long replayRuns,
                             long replayMessagesFetched,
                             long replayMessagesSkipped,
                             long replayMessagesApplied,
                             long replayMessagesFailed) {
        this.l1Hits = l1Hits;
        this.l1Misses = l1Misses;
        this.l2Hits = l2Hits;
        this.l2Misses = l2Misses;
        this.loaderCalls = loaderCalls;
        this.loaderValueCalls = loaderValueCalls;
        this.loaderPenetrationCalls = loaderPenetrationCalls;
        this.l1BackfillApplied = l1BackfillApplied;
        this.l1BackfillSkipped = l1BackfillSkipped;
        this.l1InvalidationsApplied = l1InvalidationsApplied;
        this.l1InvalidationsSkipped = l1InvalidationsSkipped;
        this.l2ReadApplyAccepted = l2ReadApplyAccepted;
        this.l2ReadApplyRejected = l2ReadApplyRejected;
        this.l2ReadApplyFailures = l2ReadApplyFailures;
        this.l2MutationApplyAccepted = l2MutationApplyAccepted;
        this.l2MutationApplyRejected = l2MutationApplyRejected;
        this.l2MutationApplyFailures = l2MutationApplyFailures;
        this.compensationSaveSuccesses = compensationSaveSuccesses;
        this.compensationSaveFailures = compensationSaveFailures;
        this.pubSubMessagesReceived = pubSubMessagesReceived;
        this.replayRuns = replayRuns;
        this.replayMessagesFetched = replayMessagesFetched;
        this.replayMessagesSkipped = replayMessagesSkipped;
        this.replayMessagesApplied = replayMessagesApplied;
        this.replayMessagesFailed = replayMessagesFailed;
    }

    public static CacheRuntimeStats empty() {
        return EMPTY;
    }

    public long getL1Hits() {
        return l1Hits;
    }

    public long getL1Misses() {
        return l1Misses;
    }

    public long getL2Hits() {
        return l2Hits;
    }

    public long getL2Misses() {
        return l2Misses;
    }

    public long getLoaderCalls() {
        return loaderCalls;
    }

    public long getLoaderValueCalls() {
        return loaderValueCalls;
    }

    public long getLoaderPenetrationCalls() {
        return loaderPenetrationCalls;
    }

    public long getL1BackfillApplied() {
        return l1BackfillApplied;
    }

    public long getL1BackfillSkipped() {
        return l1BackfillSkipped;
    }

    public long getL1InvalidationsApplied() {
        return l1InvalidationsApplied;
    }

    public long getL1InvalidationsSkipped() {
        return l1InvalidationsSkipped;
    }

    public long getL2ReadApplyAccepted() {
        return l2ReadApplyAccepted;
    }

    public long getL2ReadApplyRejected() {
        return l2ReadApplyRejected;
    }

    public long getL2ReadApplyFailures() {
        return l2ReadApplyFailures;
    }

    public long getL2MutationApplyAccepted() {
        return l2MutationApplyAccepted;
    }

    public long getL2MutationApplyRejected() {
        return l2MutationApplyRejected;
    }

    public long getL2MutationApplyFailures() {
        return l2MutationApplyFailures;
    }

    public long getCompensationSaveSuccesses() {
        return compensationSaveSuccesses;
    }

    public long getCompensationSaveFailures() {
        return compensationSaveFailures;
    }

    public long getPubSubMessagesReceived() {
        return pubSubMessagesReceived;
    }

    public long getReplayRuns() {
        return replayRuns;
    }

    public long getReplayMessagesFetched() {
        return replayMessagesFetched;
    }

    public long getReplayMessagesSkipped() {
        return replayMessagesSkipped;
    }

    public long getReplayMessagesApplied() {
        return replayMessagesApplied;
    }

    public long getReplayMessagesFailed() {
        return replayMessagesFailed;
    }
}
