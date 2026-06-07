package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageRepository;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import io.github.dk900912.multitiercache.codec.JacksonCacheCodec;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultCacheManagerWriteOrderingTest {

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
                new NoopRepository(),
                codec,
                new SingleFlight()
        );
        cacheManager.bootstrap();

        CacheKey key = CacheKey.simple("ordering:test:user:1");
        CacheMessage<String> oldMessage = new CacheMessage<>(
                key.toKeyString(),
                "value-v1",
                1L,
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

    private static final class NoopRepository implements CacheMessageRepository {
        @Override
        public void save(CacheMessage<?> message) {
        }

        @Override
        public List<CacheMessage<?>> fetchUnprocessed(int limit) {
            return List.of();
        }

        @Override
        public void markProcessed(String key, Long generation, Long version) {
        }
    }

    private static final class RecordingL1Provider implements L1Provider {
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

    private static final class BlockingL2Provider implements L2Provider {
        private final CacheCodec codec;
        private final Map<String, String> values = new HashMap<>();
        private final Map<String, Long> generations = new HashMap<>();
        private io.github.dk900912.multitiercache.api.CacheMessageListener listener;
        private final CountDownLatch applyEntered = new CountDownLatch(1);
        private final CountDownLatch allowApply = new CountDownLatch(1);

        private BlockingL2Provider(CacheConfig config) {
            this.codec = new JacksonCacheCodec();
            this.codec.initialize(config);
        }

        void seed(CacheKey key, CacheMessage<?> message) {
            values.put(CacheKeyspace.dataKey(key).toKeyString(), codec.encode(message));
            generations.put(key.toKeyString(), message.getGeneration());
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
            if (listener != null) {
                listener.onMessage(channel, message);
            }
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, io.github.dk900912.multitiercache.api.CacheMessageListener listener) {
            this.listener = listener;
            return () -> this.listener = null;
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            if (CacheLuaScripts.RESOLVE_GENERATION_LUA_SCRIPT.equals(script)) {
                String generationKey = keys.get(1);
                long current = generations.getOrDefault(generationKey, 0L);
                String type = args.getFirst();
                long generation = CacheMessageType.INSERT.getWireValue().equals(type) ? current + 1L : Math.max(current, 1L);
                generations.put(generationKey, generation);
                return generation;
            }
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
                if ("1".equals(args.get(6))) {
                    publish(args.get(5), payload);
                }
                return 1L;
            }
            throw new IllegalArgumentException("Unexpected script");
        }
    }
}
