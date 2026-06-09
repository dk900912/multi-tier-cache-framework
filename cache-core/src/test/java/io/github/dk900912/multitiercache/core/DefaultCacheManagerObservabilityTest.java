package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageRepository;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.CacheMonitor;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;
import io.github.dk900912.multitiercache.codec.JacksonCacheCodec;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DefaultCacheManagerObservabilityTest {

    @Test
    void shouldUseOnlyL1WhenL2Disabled() {
        CacheConfig config = new CacheConfig();
        config.getL2().setEnabled(false);
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));

        InMemoryL1Provider l1Provider = new InMemoryL1Provider();
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                l1Provider,
                new DisabledL2Provider(),
                new NoopRepository(),
                codec,
                new SingleFlight()
        );

        AtomicInteger loaderCalls = new AtomicInteger();
        CacheKey key = CacheKey.simple("metrics:l1-only:user:1");

        String first = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("value-v1", 1L, Duration.ofMinutes(1));
        });
        String second = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("unexpected", 2L, Duration.ofMinutes(1));
        });

        assertEquals("value-v1", first);
        assertEquals("value-v1", second);
        assertEquals(1, loaderCalls.get());

        CacheRuntimeStats stats = cacheManager.getMonitor().getRuntimeStats();
        assertEquals(1L, stats.getL1Hits());
        assertEquals(2L, stats.getL1Misses());
        assertEquals(0L, stats.getL2Hits());
        assertEquals(0L, stats.getL2Misses());
        assertEquals(0L, stats.getL2ReadFailures());
        assertEquals(1L, stats.getLoaderCalls());
        assertEquals(1L, stats.getL1BackfillApplied());
        assertEquals(0L, stats.getL2ReadApplyAccepted());
        assertEquals(0L, stats.getL2ReadApplyRejected());
        assertEquals(0L, stats.getL2ReadApplyFailures());
        assertEquals(0L, stats.getL2MutationApplyAccepted());
        assertEquals(0L, stats.getL2MutationApplyRejected());
        assertEquals(0L, stats.getL2MutationApplyFailures());
        assertEquals(0L, stats.getPubSubMessagesReceived());
    }

    @Test
    void shouldUseOnlyL2WhenL1Disabled() {
        CacheConfig config = new CacheConfig();
        config.getL1().setEnabled(false);
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));

        InMemoryL2Provider l2Provider = new InMemoryL2Provider(config);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                new DisabledL1Provider(),
                l2Provider,
                new NoopRepository(),
                codec,
                new SingleFlight()
        );

        AtomicInteger loaderCalls = new AtomicInteger();
        CacheKey key = CacheKey.simple("metrics:l2-only:user:1");

        String first = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("value-v1", 1L, Duration.ofMinutes(1));
        });
        String second = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("unexpected", 2L, Duration.ofMinutes(1));
        });

        assertEquals("value-v1", first);
        assertEquals("value-v1", second);
        assertEquals(1, loaderCalls.get());
        assertNull(cacheManager.getMonitor().getL1CacheStats(), "L1 stats should be unavailable when L1 is disabled");

        CacheRuntimeStats stats = cacheManager.getMonitor().getRuntimeStats();
        assertEquals(0L, stats.getL1Hits());
        assertEquals(0L, stats.getL1Misses());
        assertEquals(1L, stats.getL2Hits());
        assertEquals(2L, stats.getL2Misses());
        assertEquals(0L, stats.getL2ReadFailures());
        assertEquals(1L, stats.getLoaderCalls());
        assertEquals(0L, stats.getL1BackfillApplied());
        assertEquals(1L, stats.getL2ReadApplyAccepted());
        assertEquals(0L, stats.getL2ReadApplyRejected());
        assertEquals(0L, stats.getL2ReadApplyFailures());
        assertEquals(0L, stats.getL2MutationApplyAccepted());
        assertEquals(0L, stats.getL2MutationApplyRejected());
        assertEquals(0L, stats.getL2MutationApplyFailures());
        assertEquals(0L, stats.getPubSubMessagesReceived());
    }

    @Test
    void shouldExposeCoreReadWriteMetricsThroughMonitor() {
        CacheConfig config = new CacheConfig();
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));

        InMemoryL1Provider l1Provider = new InMemoryL1Provider();
        InMemoryL2Provider l2Provider = new InMemoryL2Provider(config);
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);

        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                l1Provider,
                l2Provider,
                new NoopRepository(),
                codec,
                new SingleFlight()
        );

        AtomicInteger loaderCalls = new AtomicInteger();
        CacheKey key = CacheKey.simple("metrics:user:1");

        String first = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("value-v1", 1L, Duration.ofMinutes(1));
        });
        String second = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("unexpected", 2L, Duration.ofMinutes(1));
        });
        cacheManager.update(key, "stale-v1", 1L, Duration.ofMinutes(1));
        cacheManager.evict(key, 2L, Duration.ofMinutes(1));
        String afterDelete = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("should-not-load", 99L, Duration.ofMinutes(1));
        });

        assertEquals("value-v1", first);
        assertEquals("value-v1", second);
        assertNull(afterDelete);
        assertEquals(1, loaderCalls.get());

        CacheMonitor monitor = cacheManager.getMonitor();
        CacheRuntimeStats stats = monitor.getRuntimeStats();

        assertEquals(2L, stats.getL1Hits());
        assertEquals(2L, stats.getL1Misses());
        assertEquals(0L, stats.getL2Hits());
        assertEquals(2L, stats.getL2Misses());
        assertEquals(1L, stats.getLoaderCalls());
        assertEquals(1L, stats.getLoaderValueCalls());
        assertEquals(0L, stats.getLoaderPenetrationCalls());
        assertEquals(1L, stats.getL1BackfillApplied());
        assertEquals(0L, stats.getL1BackfillSkipped());
        assertEquals(1L, stats.getL1InvalidationsApplied());
        assertEquals(0L, stats.getL1InvalidationsSkipped());
        assertEquals(1L, stats.getL2ReadApplyAccepted());
        assertEquals(0L, stats.getL2ReadApplyRejected());
        assertEquals(0L, stats.getL2ReadApplyFailures());
        assertEquals(1L, stats.getL2MutationApplyAccepted());
        assertEquals(1L, stats.getL2MutationApplyRejected());
        assertEquals(0L, stats.getL2MutationApplyFailures());
        assertEquals(0L, stats.getCompensationSaveSuccesses());
        assertEquals(0L, stats.getCompensationSaveFailures());
        assertEquals(0L, stats.getPubSubMessagesReceived());
    }

    private static final class NoopRepository implements CacheMessageRepository {
        @Override
        public void save(CacheMessage<?> message) {
        }

        @Override
        public List<CacheMessage<?>> fetchUnprocessed(int limit) {
            return List.of();
        }

        @Override
        public void markProcessed(String key, Long version) {
        }
    }

    private static final class InMemoryL1Provider implements L1Provider {
        private final Map<String, Object> store = new HashMap<>();

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

    private static final class DisabledL1Provider implements L1Provider {
        @Override
        public Object get(CacheKey key) {
            throw new AssertionError("L1 should be disabled");
        }

        @Override
        public void put(CacheKey key, Object value) {
            throw new AssertionError("L1 should be disabled");
        }

        @Override
        public void invalidate(CacheKey key) {
            throw new AssertionError("L1 should be disabled");
        }

        @Override
        public void clear() {
            throw new AssertionError("L1 should be disabled");
        }

        @Override
        public Object compute(CacheKey key, java.util.function.BiFunction<CacheKey, Object, Object> remappingFunction) {
            throw new AssertionError("L1 should be disabled");
        }
    }

    private static final class DisabledL2Provider implements L2Provider {
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
        public void publish(String channel, String message) {
            throw new AssertionError("L2 should be disabled");
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, io.github.dk900912.multitiercache.api.CacheMessageListener listener) {
            throw new AssertionError("L2 should be disabled");
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            throw new AssertionError("L2 should be disabled");
        }
    }

    private static final class InMemoryL2Provider implements L2Provider {
        private final CacheCodec codec;
        private final Map<String, String> values = new HashMap<>();

        private InMemoryL2Provider(CacheConfig config) {
            this.codec = new JacksonCacheCodec();
            this.codec.initialize(config);
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
        public void publish(String channel, String message) {
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, io.github.dk900912.multitiercache.api.CacheMessageListener listener) {
            return () -> {
            };
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            if (CacheLuaScripts.APPLY_MESSAGE_LUA_SCRIPT.equals(script)) {
                String dataKey = keys.getFirst();
                String incomingPayload = args.getFirst();
                CacheMessage<Object> incoming = codec.decodeMessage(incomingPayload, Object.class);
                String currentPayload = values.get(dataKey);
                if (currentPayload == null) {
                    values.put(dataKey, incomingPayload);
                    return 1L;
                }
                CacheMessage<Object> current = codec.decodeMessage(currentPayload, Object.class);
                if (CacheMessageVersionComparator.shouldReplace(incoming, current)) {
                    values.put(dataKey, incomingPayload);
                    return 1L;
                }
                return 0L;
            }
            throw new IllegalArgumentException("Unexpected script");
        }
    }
}
