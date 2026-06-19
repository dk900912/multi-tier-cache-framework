package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;
import io.github.dk900912.multitiercache.codec.JacksonCacheCodec;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultCacheManagerWriteOrderingTest {

    @Test
    void shouldRejectTtlThatDoesNotResolveToPositiveMilliseconds() {
        CacheConfig config = new CacheConfig();
        config.getL2().setEnabled(false);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                new RecordingL1Provider(),
                new DisabledL2Provider(),
                codec,
                new SingleFlight());
        CacheKey key = CacheKey.simple("ordering:test:invalid-ttl");

        assertThrows(IllegalArgumentException.class,
                () -> cacheManager.insert(key, "value", 1L, Duration.ofNanos(1)));
        assertThrows(IllegalArgumentException.class,
                () -> cacheManager.apply(new CacheMessage<>(
                        key.toKeyString(), "value", 1L, CacheMessageType.INSERT, 0L)));
    }

    @Test
    void shouldFallbackToLoaderWhenL2ReadFails() {
        CacheConfig config = new CacheConfig();
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));

        RecordingL1Provider l1Provider = new RecordingL1Provider();
        DegradingL2Provider l2Provider = new DegradingL2Provider(true, false);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                l1Provider,
                l2Provider,
                codec,
                new SingleFlight()
        );

        CacheKey key = CacheKey.simple("ordering:test:l2-read-failure");
        AtomicInteger loaderCalls = new AtomicInteger();
        String value = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("db-value", 1L, Duration.ofMinutes(5));
        });

        assertEquals("db-value", value);
        assertEquals(1, loaderCalls.get());
        assertInstanceOf(CacheMessage.class, l1Provider.get(key), "L1 may backfill after L2 accepts the loader result");

        CacheRuntimeStats stats = cacheManager.getMonitor().getRuntimeStats();
        assertEquals(2L, stats.getL2ReadPathFailureCount(), "initial L2 read and single-flight owner recheck should both degrade");
    }

    @Test
    void shouldReturnLoaderResultWhenReadBackfillToL2Fails() {
        CacheConfig config = new CacheConfig();
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));

        RecordingL1Provider l1Provider = new RecordingL1Provider();
        DegradingL2Provider l2Provider = new DegradingL2Provider(false, true);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                l1Provider,
                l2Provider,
                codec,
                new SingleFlight()
        );

        CacheKey key = CacheKey.simple("ordering:test:l2-read-apply-failure");
        AtomicInteger loaderCalls = new AtomicInteger();
        String value = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("db-value", 1L, Duration.ofMinutes(5));
        });

        assertEquals("db-value", value);
        assertEquals(1, loaderCalls.get());
        assertNull(l1Provider.get(key), "L1 should not backfill when L2 enabled but L2 did not accept the read result");

        CacheRuntimeStats stats = cacheManager.getMonitor().getRuntimeStats();
        assertEquals(1L, stats.getL2ReadPathFailureCount());
    }

    @Test
    void shouldPropagateMutationFailureWithoutUpdatingL1() {
        CacheConfig config = new CacheConfig();
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));
        RecordingL1Provider l1Provider = new RecordingL1Provider();
        DegradingL2Provider l2Provider = new DegradingL2Provider(false, true);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config, l1Provider, l2Provider, codec, new SingleFlight());
        CacheKey key = CacheKey.simple("ordering:test:l2-mutation-failure");

        assertThrows(IllegalStateException.class,
                () -> cacheManager.update(key, "value-v1", 1L, Duration.ofMinutes(5)));

        assertNull(l1Provider.get(key));
        assertEquals(1L, cacheManager.getMonitor().getRuntimeStats().getL2MutationFailureCount());
    }

    @Test
    void shouldInvalidateExistingLocalL1WhenL2MutationFails() {
        CacheConfig config = new CacheConfig();
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));
        RecordingL1Provider l1Provider = new RecordingL1Provider();
        DegradingL2Provider l2Provider = new DegradingL2Provider(false, true);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config, l1Provider, l2Provider, codec, new SingleFlight());
        CacheKey key = CacheKey.simple("ordering:test:l2-mutation-failure-invalidates-l1");
        l1Provider.put(key, new CacheMessage<>(
                key.toKeyString(),
                "value-v1",
                1L,
                CacheMessageType.INSERT,
                Duration.ofMinutes(5).toMillis()
        ));

        assertThrows(IllegalStateException.class,
                () -> cacheManager.update(key, "value-v2", 2L, Duration.ofMinutes(5)));

        assertNull(l1Provider.get(key), "failed L2 mutation should remove the current node's stale L1 entry");
        assertEquals(1L, cacheManager.getMonitor().getRuntimeStats().getL2MutationFailureCount());
    }

    @Test
    void shouldKeepOriginalL2FailureWhenLocalL1InvalidationAlsoFails() {
        CacheConfig config = new CacheConfig();
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));
        RuntimeException invalidateFailure = new IllegalStateException("simulated L1 invalidate failure");
        RecordingL1Provider l1Provider = new RecordingL1Provider(invalidateFailure);
        DegradingL2Provider l2Provider = new DegradingL2Provider(false, true);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config, l1Provider, l2Provider, codec, new SingleFlight());
        CacheKey key = CacheKey.simple("ordering:test:l2-mutation-and-l1-invalidate-failure");
        l1Provider.put(key, new CacheMessage<>(
                key.toKeyString(),
                "value-v1",
                1L,
                CacheMessageType.INSERT,
                Duration.ofMinutes(5).toMillis()
        ));

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> cacheManager.update(key, "value-v2", 2L, Duration.ofMinutes(5)));

        assertEquals("simulated L2 read apply failure", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertSame(invalidateFailure, failure.getSuppressed()[0]);
        assertEquals(1L, cacheManager.getMonitor().getRuntimeStats().getL2MutationFailureCount());
    }

    @Test
    void shouldKeepDeleteTombstoneInL1WhenL2Disabled() {
        CacheConfig config = new CacheConfig();
        config.getL2().setEnabled(false);
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));

        RecordingL1Provider l1Provider = new RecordingL1Provider();
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                l1Provider,
                new DisabledL2Provider(),
                codec,
                new SingleFlight()
        );

        CacheKey key = CacheKey.simple("ordering:test:l2-disabled-delete");
        l1Provider.put(key, new CacheMessage<>(
                key.toKeyString(),
                "value-v1",
                1L,
                CacheMessageType.INSERT,
                Duration.ofMinutes(5).toMillis()
        ));

        cacheManager.evict(key, 2L, Duration.ofMinutes(5));

        CacheMessage<?> tombstone = (CacheMessage<?>) assertInstanceOf(CacheMessage.class, l1Provider.get(key));
        assertEquals(CacheMessageType.DELETE, tombstone.getType());
        assertEquals(2L, tombstone.getVersion());

        AtomicInteger loaderCalls = new AtomicInteger();
        String cached = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("stale-v1", 1L, Duration.ofMinutes(5));
        });

        assertNull(cached);
        assertEquals(0, loaderCalls.get(), "L1 tombstone should block stale backfill when L2 is disabled");
        CacheMessage<?> afterRead = (CacheMessage<?>) assertInstanceOf(CacheMessage.class, l1Provider.get(key));
        assertEquals(CacheMessageType.DELETE, afterRead.getType());
        assertEquals(2L, afterRead.getVersion());
    }

    @Test
    void shouldInvalidateExistingL1FromPubSubWithoutWarmingAbsentKeys() {
        CacheConfig config = new CacheConfig();
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));

        RecordingL1Provider l1Provider = new RecordingL1Provider();
        BlockingL2Provider l2Provider = new BlockingL2Provider(config);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                l1Provider,
                l2Provider,
                codec,
                new SingleFlight()
        );
        cacheManager.bootstrap();

        CacheKey key = CacheKey.simple("ordering:test:pubsub-converge");
        CacheMessage<String> remoteUpdate = new CacheMessage<>(
                key.toKeyString(),
                "value-v2",
                2L,
                CacheMessageType.UPDATE,
                Duration.ofMinutes(5).toMillis()
        );

        l2Provider.publish(config.getL2().getMutationChannelName(), codec.encode(remoteUpdate), L2PubSubMode.STANDARD);
        assertNull(l1Provider.get(key), "remote Pub/Sub should not prewarm an absent L1 entry");

        l1Provider.put(key, new CacheMessage<>(
                key.toKeyString(),
                "value-v1",
                1L,
                CacheMessageType.INSERT,
                Duration.ofMinutes(5).toMillis()
        ));

        l2Provider.publish(config.getL2().getMutationChannelName(), codec.encode(remoteUpdate), L2PubSubMode.STANDARD);

        assertNull(l1Provider.get(key), "remote Pub/Sub should invalidate stale L1 instead of storing the payload");
    }

    @Test
    void shouldKeepLocalL1VisibleUntilL2ApplyCompletes() throws Exception {
        CacheConfig config = new CacheConfig();
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));

        RecordingL1Provider l1Provider = new RecordingL1Provider();
        BlockingL2Provider l2Provider = new BlockingL2Provider(config);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);

        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                l1Provider,
                l2Provider,
                codec,
                new SingleFlight()
        );
        cacheManager.bootstrap();

        CacheKey key = CacheKey.simple("ordering:test:user:1");
        CacheMessage<String> oldMessage = new CacheMessage<>(
                key.toKeyString(),
                "value-v1",
                1L,
                CacheMessageType.INSERT,
                Duration.ofMinutes(5).toMillis()
        );
        l1Provider.put(key, oldMessage);
        l2Provider.seed(key, oldMessage);

        Thread writer = new Thread(() ->
                cacheManager.update(key, "value-v2", 2L, Duration.ofMinutes(5)), "writer-thread");
        writer.start();

        assertTrue(l2Provider.applyEntered.await(5, TimeUnit.SECONDS), "writer should enter L2 apply");
        Object visibleDuringApply = l1Provider.get(key);
        assertNotNull(visibleDuringApply, "local L1 should remain visible until L2 apply completes");

        l2Provider.allowApply.countDown();
        writer.join(5000);

        AtomicInteger loaderCalls = new AtomicInteger();
        String latest = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("unexpected-loader", 999L, Duration.ofMinutes(5));
        });

        assertEquals("value-v2", latest);
        assertEquals(0, loaderCalls.get(), "after write completes, current node should observe fresh value from cache");
    }

    private static final class RecordingL1Provider implements L1Provider {
        private final Map<String, Object> store = new HashMap<>();
        private final RuntimeException invalidateFailure;

        private RecordingL1Provider() {
            this(null);
        }

        private RecordingL1Provider(RuntimeException invalidateFailure) {
            this.invalidateFailure = invalidateFailure;
        }

        @Override
        public Object get(CacheKey key) {
            return store.get(key.toKeyString());
        }

        @Override
        public void put(CacheKey key, Object value) {
            store.put(key.toKeyString(), value);
        }

        @Override
        public void invalidate(CacheKey key) {
            if (invalidateFailure != null) {
                throw invalidateFailure;
            }
            store.remove(key.toKeyString());
        }

        @Override
        public void clear() {
            store.clear();
        }

        @Override
        public Object compute(CacheKey key, java.util.function.BiFunction<CacheKey, Object, Object> remappingFunction) {
            String keyString = key.toKeyString();
            Object newValue = remappingFunction.apply(key, store.get(keyString));
            if (newValue == null) {
                store.remove(keyString);
            } else {
                store.put(keyString, newValue);
            }
            return newValue;
        }
    }

    private static final class BlockingL2Provider implements L2Provider {
        private final CacheCodec codec;
        private final Map<String, String> values = new HashMap<>();
        private io.github.dk900912.multitiercache.api.CacheMessageListener listener;
        private final CountDownLatch applyEntered = new CountDownLatch(1);
        private final CountDownLatch allowApply = new CountDownLatch(1);

        private BlockingL2Provider(CacheConfig config) {
            this.codec = new JacksonCacheCodec();
            this.codec.initialize(config);
        }

        @Override
        public void initialize(CacheConfig.L2Config config) {
        }

        void seed(CacheKey key, CacheMessage<?> message) {
            values.put(CacheKeyspace.dataKey(key).toKeyString(), codec.encode(message));
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
            if (listener != null) {
                listener.onMessage(channel, message);
            }
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, io.github.dk900912.multitiercache.api.CacheMessageListener listener, L2PubSubMode mode) {
            this.listener = listener;
            return () -> this.listener = null;
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            if (CacheLuaScripts.APPLY_MESSAGE_LUA_SCRIPT.equals(script)) {
                applyEntered.countDown();
                try {
                    assertTrue(allowApply.await(5, TimeUnit.SECONDS), "test should release L2 apply");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                String payload = args.getFirst();
                values.put(keys.getFirst(), payload);
                if ("1".equals(args.get(5))) {
                    publish(args.get(4), payload, L2PubSubMode.STANDARD);
                }
                return 1L;
            }
            throw new IllegalArgumentException("Unexpected script");
        }
    }

    private static final class DisabledL2Provider implements L2Provider {
        @Override
        public void initialize(CacheConfig.L2Config config) {
            throw new AssertionError("L2 should be disabled");
        }

        @Override
        public String get(CacheKey key) {
            throw new AssertionError("L2 should be disabled");
        }

        @Override
        public void set(CacheKey key, String value, Duration ttl) {
            throw new AssertionError("L2 should be disabled");
        }

        @Override
        public void delete(CacheKey key) {
            throw new AssertionError("L2 should be disabled");
        }

        @Override
        public void publish(String channel, String message, L2PubSubMode mode) {
            throw new AssertionError("L2 should be disabled");
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, io.github.dk900912.multitiercache.api.CacheMessageListener listener, L2PubSubMode mode) {
            throw new AssertionError("L2 should be disabled");
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            throw new AssertionError("L2 should be disabled");
        }
    }

    private static final class DegradingL2Provider implements L2Provider {
        private final boolean failGet;
        private final boolean failEval;
        private final Map<String, String> values = new HashMap<>();

        private DegradingL2Provider(boolean failGet, boolean failEval) {
            this.failGet = failGet;
            this.failEval = failEval;
        }

        @Override
        public void initialize(CacheConfig.L2Config config) {
        }

        @Override
        public String get(CacheKey key) {
            if (failGet) {
                throw new IllegalStateException("simulated L2 read failure");
            }
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
        public CacheMessageSubscription subscribe(String channel, io.github.dk900912.multitiercache.api.CacheMessageListener listener, L2PubSubMode mode) {
            return () -> {
            };
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            if (!CacheLuaScripts.APPLY_MESSAGE_LUA_SCRIPT.equals(script)) {
                throw new IllegalArgumentException("Unexpected script");
            }
            if (failEval) {
                throw new IllegalStateException("simulated L2 read apply failure");
            }
            values.put(keys.getFirst(), args.getFirst());
            return 1L;
        }
    }
}
