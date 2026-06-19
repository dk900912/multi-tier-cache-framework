package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageDeliveryEvent;
import io.github.dk900912.multitiercache.api.CacheMessageDeliveryEventType;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;
import io.github.dk900912.multitiercache.codec.JacksonCacheCodec;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultCacheManagerL1RecoveryTest {

    @Test
    void debouncesRepeatedOverloadEventsAndClearsL1OnlyOnce() throws Exception {
        TestContext context = new TestContext(0);
        try {
            CacheKey key = CacheKey.simple("recovery:event-storm");
            context.manager.update(key, "v1", 1L, Duration.ofMinutes(1));
            context.manager.bootstrap();

            CacheMessageDeliveryEvent overloaded = event(
                    CacheMessageDeliveryEventType.PROCESSING_OVERLOADED, 1L);
            for (int i = 0; i < 10_000; i++) {
                context.l2Provider.delivery(overloaded);
            }

            context.manager.update(key, "v2", 2L, Duration.ofMinutes(1));
            assertEquals("v2", context.manager.get(key,
                    () -> CacheLoadResult.of("unexpected", 3L, Duration.ofMinutes(1))));

            context.l2Provider.delivery(event(
                    CacheMessageDeliveryEventType.PROCESSING_RECOVERED, 10_000L));
            await(() -> context.l1Provider.successfulClears.get() == 1);
            TimeUnit.MILLISECONDS.sleep(100);

            CacheRuntimeStats stats = context.manager.getMonitor().getRuntimeStats();
            assertEquals(10_000L, stats.getPubSubDroppedMessageCount());
            assertTrue(stats.getL1UntrustedBypassCount() > 0L);
            assertEquals(1, context.l1Provider.successfulClears.get());
        } finally {
            context.manager.shutdown();
        }
    }

    @Test
    void waitsForEveryDegradationReasonBeforeRecovering() throws Exception {
        TestContext context = new TestContext(0);
        try {
            context.manager.bootstrap();
            context.l2Provider.delivery(event(CacheMessageDeliveryEventType.PROCESSING_OVERLOADED, 1L));
            context.l2Provider.delivery(event(CacheMessageDeliveryEventType.SUBSCRIPTION_INTERRUPTED, 0L));
            context.l2Provider.delivery(event(CacheMessageDeliveryEventType.PROCESSING_RECOVERED, 1L));

            TimeUnit.MILLISECONDS.sleep(150);
            assertEquals(0, context.l1Provider.successfulClears.get());

            context.l2Provider.delivery(event(CacheMessageDeliveryEventType.SUBSCRIPTION_RESTORED, 0L));
            await(() -> context.l1Provider.successfulClears.get() == 1);

            CacheRuntimeStats stats = context.manager.getMonitor().getRuntimeStats();
            assertEquals(1L, stats.getPubSubInterruptionCount());
        } finally {
            context.manager.shutdown();
        }
    }

    @Test
    void retriesFailedClearWithoutStartingConcurrentRecoveryTasks() throws Exception {
        TestContext context = new TestContext(1);
        try {
            context.manager.bootstrap();
            context.l2Provider.delivery(event(CacheMessageDeliveryEventType.PROCESSING_OVERLOADED, 1L));
            context.l2Provider.delivery(event(CacheMessageDeliveryEventType.PROCESSING_RECOVERED, 1L));

            await(() -> context.l1Provider.successfulClears.get() == 1);
            assertEquals(2, context.l1Provider.clearAttempts.get());
            CacheRuntimeStats stats = context.manager.getMonitor().getRuntimeStats();
            assertEquals(1L, stats.getL1RecoveryClearFailureCount());
        } finally {
            context.manager.shutdown();
        }
    }

    @Test
    void staleRecoveryEventCannotCloseANewerOverloadEpisode() throws Exception {
        TestContext context = new TestContext(0);
        try {
            context.manager.bootstrap();
            context.l2Provider.delivery(new CacheMessageDeliveryEvent(
                    "multi-tier-cache-mutation",
                    CacheMessageDeliveryEventType.PROCESSING_OVERLOADED,
                    1L,
                    1L,
                    null));
            context.l2Provider.delivery(new CacheMessageDeliveryEvent(
                    "multi-tier-cache-mutation",
                    CacheMessageDeliveryEventType.PROCESSING_OVERLOADED,
                    2L,
                    1L,
                    null));
            context.l2Provider.delivery(new CacheMessageDeliveryEvent(
                    "multi-tier-cache-mutation",
                    CacheMessageDeliveryEventType.PROCESSING_RECOVERED,
                    1L,
                    1L,
                    null));

            TimeUnit.MILLISECONDS.sleep(150);
            assertEquals(0, context.l1Provider.successfulClears.get());

            context.l2Provider.delivery(new CacheMessageDeliveryEvent(
                    "multi-tier-cache-mutation",
                    CacheMessageDeliveryEventType.PROCESSING_RECOVERED,
                    2L,
                    1L,
                    null));
            await(() -> context.l1Provider.successfulClears.get() == 1);
            assertEquals(2L, context.manager.getMonitor().getRuntimeStats().getPubSubDroppedMessageCount());
        } finally {
            context.manager.shutdown();
        }
    }

    private static CacheMessageDeliveryEvent event(CacheMessageDeliveryEventType type, long dropped) {
        long episodeId = switch (type) {
            case PROCESSING_OVERLOADED, PROCESSING_RECOVERED -> 1L;
            case SUBSCRIPTION_INTERRUPTED, SUBSCRIPTION_RESTORED -> 0L;
        };
        return new CacheMessageDeliveryEvent(
                "multi-tier-cache-mutation", type, episodeId, dropped, null);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "Condition was not satisfied before timeout");
    }

    private static final class TestContext {
        private final CountingL1Provider l1Provider;
        private final EventingL2Provider l2Provider;
        private final DefaultCacheManager manager;

        private TestContext(int clearFailures) {
            CacheConfig config = new CacheConfig();
            config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));
            CacheCodec codec = new JacksonCacheCodec();
            codec.initialize(config);
            this.l1Provider = new CountingL1Provider(clearFailures);
            this.l2Provider = new EventingL2Provider(codec);
            this.manager = new DefaultCacheManager(
                    config,
                    l1Provider,
                    l2Provider,
                    codec,
                    new SingleFlight());
        }
    }

    private static final class CountingL1Provider implements L1Provider {
        private final Map<String, Object> values = new ConcurrentHashMap<>();
        private final AtomicInteger remainingClearFailures;
        private final AtomicInteger clearAttempts = new AtomicInteger();
        private final AtomicInteger successfulClears = new AtomicInteger();

        private CountingL1Provider(int clearFailures) {
            this.remainingClearFailures = new AtomicInteger(clearFailures);
        }

        @Override
        public Object get(CacheKey key) {
            return values.get(key.toKeyString());
        }

        @Override
        public void put(CacheKey key, Object value) {
            values.put(key.toKeyString(), value);
        }

        @Override
        public void invalidate(CacheKey key) {
            values.remove(key.toKeyString());
        }

        @Override
        public void clear() {
            clearAttempts.incrementAndGet();
            if (remainingClearFailures.getAndUpdate(current -> Math.max(0, current - 1)) > 0) {
                throw new IllegalStateException("simulated clear failure");
            }
            values.clear();
            successfulClears.incrementAndGet();
        }

        @Override
        public Object compute(CacheKey key, java.util.function.BiFunction<CacheKey, Object, Object> function) {
            return values.compute(key.toKeyString(), (ignored, current) -> function.apply(key, current));
        }
    }

    private static final class EventingL2Provider implements L2Provider {
        private final CacheCodec codec;
        private final Map<String, String> values = new ConcurrentHashMap<>();
        private volatile CacheMessageListener listener;

        private EventingL2Provider(CacheCodec codec) {
            this.codec = codec;
        }

        @Override
        public void initialize(CacheConfig.L2Config config) {
        }

        private void delivery(CacheMessageDeliveryEvent event) {
            listener.onDeliveryEvent(event);
        }

        @Override
        public String get(CacheKey key) {
            return values.get(key.toKeyString());
        }

        @Override
        public void set(CacheKey key, String value, Duration ttl) {
            values.put(key.toKeyString(), value);
        }

        @Override
        public void delete(CacheKey key) {
            values.remove(key.toKeyString());
        }

        @Override
        public void publish(String channel, String message, L2PubSubMode mode) {
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener, L2PubSubMode mode) {
            this.listener = listener;
            return () -> this.listener = null;
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            String dataKey = keys.getFirst();
            String incomingPayload = args.getFirst();
            CacheMessage<Object> incoming = codec.decodeMessage(incomingPayload, Object.class);
            return values.compute(dataKey, (ignored, currentPayload) -> {
                if (currentPayload == null) {
                    return incomingPayload;
                }
                CacheMessage<Object> current = codec.decodeMessage(currentPayload, Object.class);
                return CacheMessageVersionComparator.shouldReplace(incoming, current)
                        ? incomingPayload
                        : currentPayload;
            }).equals(incomingPayload) ? 1L : 0L;
        }
    }

}
