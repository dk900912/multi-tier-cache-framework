package io.github.dk900912.multitiercache.spi.support;

import io.github.dk900912.multitiercache.api.CacheMessageDeliveryEvent;
import io.github.dk900912.multitiercache.api.CacheMessageDeliveryEventType;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared bounded dispatcher for L2 Pub/Sub callbacks.
 * <p>
 * Rejections are aggregated into overload episodes without allocating an exception per message.
 * This class is infrastructure for built-in L2 providers and is not a reliable message queue.
 *
 * @author dukui
 */
public final class PubSubMessageDispatcher implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageDispatcher.class);

    private final String name;
    private final int capacity;
    private final int recoveryLowWatermark;
    private final long recoveryQuietNanos;
    private final ThreadPoolExecutor executor;
    private final ScheduledThreadPoolExecutor recoveryScheduler;
    private final AtomicBoolean recoveryCheckScheduled = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object overloadLock = new Object();
    private final Map<ListenerChannel, OverloadEpisode> affectedListeners = new HashMap<>();

    private boolean overloaded;
    private long overloadStartedNanos;
    private long lastRejectedNanos;
    private long totalDropped;
    private long episodeSequence;
    private long activeEpisodeId;

    public PubSubMessageDispatcher(CacheConfig.Subscriber subscriber, String name) {
        Objects.requireNonNull(subscriber, "Subscriber config cannot be null");
        this.name = Objects.requireNonNull(name, "Dispatcher name cannot be null");
        this.capacity = subscriber.getCapacity();
        double recoveryLowWatermarkRatio = subscriber.getRecoveryLowWatermarkRatio();
        if (!Double.isFinite(recoveryLowWatermarkRatio)
                || recoveryLowWatermarkRatio <= 0.0d
                || recoveryLowWatermarkRatio >= 1.0d) {
            throw new IllegalArgumentException(
                    "Recovery low-watermark ratio must be greater than 0 and less than 1");
        }
        Duration recoveryQuietPeriod = Objects.requireNonNull(
                subscriber.getRecoveryQuietPeriod(), "Recovery quiet period cannot be null");
        if (recoveryQuietPeriod.isZero() || recoveryQuietPeriod.isNegative()) {
            throw new IllegalArgumentException("Recovery quiet period must be positive");
        }
        this.recoveryLowWatermark = (int) Math.floor(capacity * recoveryLowWatermarkRatio);
        this.recoveryQuietNanos = toSaturatedNanos(recoveryQuietPeriod);
        this.recoveryScheduler = new ScheduledThreadPoolExecutor(
                1, Thread.ofPlatform().daemon().name(name + "-recovery-", 1).factory());
        this.recoveryScheduler.setRemoveOnCancelPolicy(true);
        this.executor = new ThreadPoolExecutor(
                subscriber.getCorePoolSize(),
                subscriber.getMaximumPoolSize(),
                subscriber.getKeepAliveTime().toMillis(),
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(capacity),
                Thread.ofPlatform().daemon().name(name + "-worker-", 1).factory(),
                (task, ignored) -> reject(task));
    }

    public void dispatch(String channel, String payload, CacheMessageListener listener) {
        Objects.requireNonNull(channel, "Pub/Sub channel cannot be null");
        Objects.requireNonNull(payload, "Pub/Sub payload cannot be null");
        Objects.requireNonNull(listener, "Cache message listener cannot be null");
        if (closed.get()) {
            return;
        }
        executor.execute(new DispatchTask(channel, payload, listener));
    }

    public void notifyDeliveryEvent(CacheMessageListener listener, CacheMessageDeliveryEvent event) {
        Objects.requireNonNull(listener, "Cache message listener cannot be null");
        Objects.requireNonNull(event, "Delivery event cannot be null");
        try {
            listener.onDeliveryEvent(event);
        } catch (RuntimeException e) {
            LOGGER.error("{} listener failed while handling delivery event {} for channel {}",
                    name, event.type(), event.channel(), e);
        }
    }

    private void reject(Runnable task) {
        if (closed.get() || executor.isShutdown() || !(task instanceof DispatchTask dispatchTask)) {
            return;
        }

        CacheMessageDeliveryEvent overloadEvent = null;
        boolean logOverload = false;
        long now = System.nanoTime();
        synchronized (overloadLock) {
            if (!overloaded) {
                overloaded = true;
                activeEpisodeId = ++episodeSequence;
                overloadStartedNanos = now;
                totalDropped = 0L;
                logOverload = true;
            }
            lastRejectedNanos = now;
            totalDropped++;
            ListenerChannel key = new ListenerChannel(dispatchTask.listener, dispatchTask.channel);
            OverloadEpisode episode = affectedListeners.get(key);
            if (episode == null) {
                affectedListeners.put(key, new OverloadEpisode(1L));
                overloadEvent = new CacheMessageDeliveryEvent(
                        dispatchTask.channel,
                        CacheMessageDeliveryEventType.PROCESSING_OVERLOADED,
                        activeEpisodeId,
                        1L,
                        null);
            } else {
                episode.dropped++;
            }
        }

        if (logOverload) {
            LOGGER.warn("{} Pub/Sub processing overloaded; capacity={}, recoveryLowWatermark={}",
                    name, capacity, recoveryLowWatermark);
        }
        if (overloadEvent != null) {
            notifyDeliveryEvent(dispatchTask.listener, overloadEvent);
        }
        scheduleRecoveryCheck(recoveryQuietNanos);
    }

    private static long toSaturatedNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private void scheduleRecoveryCheck(long delayNanos) {
        if (closed.get() || !recoveryCheckScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            recoveryScheduler.schedule(
                    this::checkRecovery,
                    Math.max(1L, delayNanos),
                    TimeUnit.NANOSECONDS);
        } catch (RuntimeException e) {
            recoveryCheckScheduled.set(false);
            if (!closed.get()) {
                LOGGER.error("{} failed to schedule Pub/Sub overload recovery check", name, e);
            }
        }
    }

    private void checkRecovery() {
        recoveryCheckScheduled.set(false);
        if (closed.get()) {
            return;
        }

        List<RecoveredListener> recoveredListeners = null;
        long dropped = 0L;
        long durationNanos = 0L;
        long recoveredEpisodeId = 0L;
        long nextDelayNanos = recoveryQuietNanos;
        long now = System.nanoTime();
        synchronized (overloadLock) {
            if (!overloaded) {
                return;
            }
            long quietNanos = now - lastRejectedNanos;
            boolean quiet = quietNanos >= recoveryQuietNanos;
            boolean belowLowWatermark = executor.getQueue().size() <= recoveryLowWatermark;
            if (quiet && belowLowWatermark) {
                overloaded = false;
                recoveredEpisodeId = activeEpisodeId;
                dropped = totalDropped;
                durationNanos = now - overloadStartedNanos;
                recoveredListeners = new ArrayList<>(affectedListeners.size());
                for (Map.Entry<ListenerChannel, OverloadEpisode> entry : affectedListeners.entrySet()) {
                    recoveredListeners.add(new RecoveredListener(
                            entry.getKey().listener,
                            entry.getKey().channel,
                            entry.getValue().dropped));
                }
                affectedListeners.clear();
                totalDropped = 0L;
            } else if (!quiet) {
                nextDelayNanos = recoveryQuietNanos - quietNanos;
            }
        }

        if (recoveredListeners == null) {
            scheduleRecoveryCheck(nextDelayNanos);
            return;
        }

        LOGGER.info("{} Pub/Sub processing recovered after {} ms; droppedMessages={}",
                name, TimeUnit.NANOSECONDS.toMillis(durationNanos), dropped);
        for (RecoveredListener recovered : recoveredListeners) {
            notifyDeliveryEvent(recovered.listener, new CacheMessageDeliveryEvent(
                    recovered.channel,
                    CacheMessageDeliveryEventType.PROCESSING_RECOVERED,
                    recoveredEpisodeId,
                    recovered.dropped,
                    null));
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        recoveryScheduler.shutdownNow();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(3, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        synchronized (overloadLock) {
            affectedListeners.clear();
            overloaded = false;
        }
    }

    private final class DispatchTask implements Runnable {
        private final String channel;
        private final String payload;
        private final CacheMessageListener listener;

        private DispatchTask(String channel, String payload, CacheMessageListener listener) {
            this.channel = channel;
            this.payload = payload;
            this.listener = listener;
        }

        @Override
        public void run() {
            try {
                listener.onMessage(channel, payload);
            } catch (RuntimeException e) {
                LOGGER.error("{} listener failed while processing channel {}", name, channel, e);
            }
        }
    }

    private static final class ListenerChannel {
        private final CacheMessageListener listener;
        private final String channel;

        private ListenerChannel(CacheMessageListener listener, String channel) {
            this.listener = listener;
            this.channel = channel;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ListenerChannel that
                    && listener == that.listener
                    && channel.equals(that.channel);
        }

        @Override
        public int hashCode() {
            return 31 * System.identityHashCode(listener) + channel.hashCode();
        }
    }

    private static final class OverloadEpisode {
        private long dropped;

        private OverloadEpisode(long dropped) {
            this.dropped = dropped;
        }
    }

    private record RecoveredListener(CacheMessageListener listener, String channel, long dropped) {
    }
}
