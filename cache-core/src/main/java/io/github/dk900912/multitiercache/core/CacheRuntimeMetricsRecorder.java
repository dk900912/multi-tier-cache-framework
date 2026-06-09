package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;

import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe runtime metrics recorder for cache observability.
 *
 * @author dukui
 */
final class CacheRuntimeMetricsRecorder {

    private final LongAdder l1Hits = new LongAdder();
    private final LongAdder l1Misses = new LongAdder();
    private final LongAdder l2Hits = new LongAdder();
    private final LongAdder l2Misses = new LongAdder();
    private final LongAdder loaderCalls = new LongAdder();
    private final LongAdder loaderValueCalls = new LongAdder();
    private final LongAdder loaderPenetrationCalls = new LongAdder();
    private final LongAdder l1BackfillApplied = new LongAdder();
    private final LongAdder l1BackfillSkipped = new LongAdder();
    private final LongAdder l1InvalidationsApplied = new LongAdder();
    private final LongAdder l1InvalidationsSkipped = new LongAdder();
    private final LongAdder l2ReadFailures = new LongAdder();
    private final LongAdder l2ReadApplyAccepted = new LongAdder();
    private final LongAdder l2ReadApplyRejected = new LongAdder();
    private final LongAdder l2ReadApplyFailures = new LongAdder();
    private final LongAdder l2MutationApplyAccepted = new LongAdder();
    private final LongAdder l2MutationApplyRejected = new LongAdder();
    private final LongAdder l2MutationApplyFailures = new LongAdder();
    private final LongAdder compensationSaveSuccesses = new LongAdder();
    private final LongAdder compensationSaveFailures = new LongAdder();
    private final LongAdder pubSubMessagesReceived = new LongAdder();
    private final LongAdder replayRuns = new LongAdder();
    private final LongAdder replayMessagesFetched = new LongAdder();
    private final LongAdder replayMessagesSkipped = new LongAdder();
    private final LongAdder replayMessagesApplied = new LongAdder();
    private final LongAdder replayMessagesFailed = new LongAdder();

    void recordL1Hit() {
        l1Hits.increment();
    }

    void recordL1Miss() {
        l1Misses.increment();
    }

    void recordL2Hit() {
        l2Hits.increment();
    }

    void recordL2Miss() {
        l2Misses.increment();
    }

    void recordLoaderCall(boolean penetration) {
        loaderCalls.increment();
        if (penetration) {
            loaderPenetrationCalls.increment();
        } else {
            loaderValueCalls.increment();
        }
    }

    void recordL1BackfillApplied() {
        l1BackfillApplied.increment();
    }

    void recordL1BackfillSkipped() {
        l1BackfillSkipped.increment();
    }

    void recordL1InvalidationApplied() {
        l1InvalidationsApplied.increment();
    }

    void recordL1InvalidationSkipped() {
        l1InvalidationsSkipped.increment();
    }

    void recordL2ReadFailure() {
        l2ReadFailures.increment();
    }

    void recordL2ReadApplyAccepted() {
        l2ReadApplyAccepted.increment();
    }

    void recordL2ReadApplyRejected() {
        l2ReadApplyRejected.increment();
    }

    void recordL2ReadApplyFailure() {
        l2ReadApplyFailures.increment();
    }

    void recordL2MutationApplyAccepted() {
        l2MutationApplyAccepted.increment();
    }

    void recordL2MutationApplyRejected() {
        l2MutationApplyRejected.increment();
    }

    void recordL2MutationApplyFailure() {
        l2MutationApplyFailures.increment();
    }

    void recordCompensationSaveSuccess() {
        compensationSaveSuccesses.increment();
    }

    void recordCompensationSaveFailure() {
        compensationSaveFailures.increment();
    }

    void recordPubSubMessageReceived() {
        pubSubMessagesReceived.increment();
    }

    void recordReplayRun() {
        replayRuns.increment();
    }

    void recordReplayMessagesFetched(int count) {
        replayMessagesFetched.add(count);
    }

    void recordReplayMessageSkipped() {
        replayMessagesSkipped.increment();
    }

    void recordReplayMessageApplied() {
        replayMessagesApplied.increment();
    }

    void recordReplayMessageFailed() {
        replayMessagesFailed.increment();
    }

    CacheRuntimeStats snapshot() {
        return new CacheRuntimeStats(
                l1Hits.sum(),
                l1Misses.sum(),
                l2Hits.sum(),
                l2Misses.sum(),
                loaderCalls.sum(),
                loaderValueCalls.sum(),
                loaderPenetrationCalls.sum(),
                l1BackfillApplied.sum(),
                l1BackfillSkipped.sum(),
                l1InvalidationsApplied.sum(),
                l1InvalidationsSkipped.sum(),
                l2ReadFailures.sum(),
                l2ReadApplyAccepted.sum(),
                l2ReadApplyRejected.sum(),
                l2ReadApplyFailures.sum(),
                l2MutationApplyAccepted.sum(),
                l2MutationApplyRejected.sum(),
                l2MutationApplyFailures.sum(),
                compensationSaveSuccesses.sum(),
                compensationSaveFailures.sum(),
                pubSubMessagesReceived.sum(),
                replayRuns.sum(),
                replayMessagesFetched.sum(),
                replayMessagesSkipped.sum(),
                replayMessagesApplied.sum(),
                replayMessagesFailed.sum()
        );
    }
}
