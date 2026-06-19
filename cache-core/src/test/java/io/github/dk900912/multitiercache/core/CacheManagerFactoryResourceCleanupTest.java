package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.exception.CacheCodecException;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CacheManagerFactoryResourceCleanupTest {

    @Test
    void shouldCloseInitializedResourcesInReverseOrderWhenCodecInitializationFails() {
        List<String> events = new ArrayList<>();
        RecordingL1Provider l1 = new RecordingL1Provider(events);
        RecordingL2Provider l2 = new RecordingL2Provider(events);
        RecordingCodec codec = new RecordingCodec(events);
        codec.failOnInitialize = true;

        assertThrows(IllegalStateException.class,
                () -> create(config(), l1, l2, codec));

        assertEquals(List.of(
                "l1.initialize",
                "l2.initialize",
                "codec.initialize",
                "codec.close",
                "l2.close",
                "l1.close"
        ), events);
    }

    @Test
    void shouldCloseFailingCandidateAndEarlierResourcesWhenProviderInitializationFails() {
        List<String> events = new ArrayList<>();
        RecordingL1Provider l1 = new RecordingL1Provider(events);
        RecordingL2Provider l2 = new RecordingL2Provider(events);
        l2.failOnInitialize = true;

        assertThrows(IllegalStateException.class,
                () -> create(config(), l1, l2, new RecordingCodec(events)));

        assertEquals(List.of(
                "l1.initialize",
                "l2.initialize",
                "l2.close",
                "l1.close"
        ), events);
    }

    @Test
    void shouldPreserveInitializationFailureAndSuppressCleanupFailure() {
        List<String> events = new ArrayList<>();
        RecordingL1Provider l1 = new RecordingL1Provider(events);
        RecordingL2Provider l2 = new RecordingL2Provider(events);
        RecordingCodec codec = new RecordingCodec(events);
        codec.failOnInitialize = true;
        l2.failOnClose = true;

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> create(config(), l1, l2, codec));

        assertEquals("Failed to initialize CacheCodec: " + RecordingCodec.class.getName(), failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertEquals("l2 close failure", failure.getSuppressed()[0].getMessage());
    }

    @Test
    void shouldCloseOwnedResourcesOnceInReverseOrderOnNormalShutdown() {
        List<String> events = new ArrayList<>();
        RecordingL1Provider l1 = new RecordingL1Provider(events);
        RecordingL2Provider l2 = new RecordingL2Provider(events);
        RecordingCodec codec = new RecordingCodec(events);
        CacheManager manager = create(config(), l1, l2, codec);
        events.clear();

        manager.shutdown();
        manager.shutdown();

        assertEquals(List.of("codec.close", "l2.close", "l1.close"), events);
    }

    @Test
    void shouldCloseInitializedResourcesWhenExecutorCreationFails() {
        List<String> events = new ArrayList<>();
        RecordingL1Provider l1 = new RecordingL1Provider(events);
        RecordingL2Provider l2 = new RecordingL2Provider(events);
        RecordingCodec codec = new RecordingCodec(events);

        IllegalStateException failure = assertThrows(IllegalStateException.class, () ->
                CacheManagerFactory.create(
                        config(),
                        new FixedProviderLoader(l1, l2, codec),
                        () -> {
                            throw new IllegalStateException("executor creation failure");
                        }));

        assertEquals("executor creation failure", failure.getMessage());
        assertEquals(List.of(
                "l1.initialize",
                "l2.initialize",
                "codec.initialize",
                "codec.close",
                "l2.close",
                "l1.close"
        ), events);
    }

    private static CacheManager create(CacheConfig config,
                                       L1Provider l1,
                                       L2Provider l2,
                                       CacheCodec codec) {
        return CacheManagerFactory.create(
                config,
                new FixedProviderLoader(l1, l2, codec),
                Executors::newSingleThreadExecutor);
    }

    private static CacheConfig config() {
        CacheConfig config = new CacheConfig();
        config.getL1().setProvider(CacheConfig.L1ProviderType.JDK);
        config.getL1().setExpireAfterWrite(Duration.ofMinutes(5));
        config.getL2().setProvider(CacheConfig.L2ProviderType.JEDIS);
        config.getL2().setHosts(List.of("127.0.0.1:6379"));
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));
        return config;
    }

    private record FixedProviderLoader(L1Provider l1, L2Provider l2, CacheCodec codec)
            implements CacheManagerFactory.ProviderLoader {
        @Override
        @SuppressWarnings("unchecked")
        public <T> List<T> load(Class<T> providerType) {
            if (providerType == L1Provider.class) {
                return List.of((T) l1);
            }
            if (providerType == L2Provider.class) {
                return List.of((T) l2);
            }
            if (providerType == CacheCodec.class) {
                return List.of((T) codec);
            }
            return List.of();
        }
    }

    private static final class RecordingL1Provider implements L1Provider, AutoCloseable {
        private final List<String> events;

        private RecordingL1Provider(List<String> events) {
            this.events = events;
        }

        @Override
        public void initialize(CacheConfig.L1Config config) {
            events.add("l1.initialize");
        }

        @Override
        public CacheConfig.L1ProviderType providerType() {
            return CacheConfig.L1ProviderType.JDK;
        }

        @Override
        public Object get(CacheKey key) {
            return null;
        }

        @Override
        public void put(CacheKey key, Object value) {
        }

        @Override
        public void invalidate(CacheKey key) {
        }

        @Override
        public void clear() {
        }

        @Override
        public void close() {
            events.add("l1.close");
        }
    }

    private static final class RecordingL2Provider implements L2Provider, AutoCloseable {
        private final List<String> events;
        private boolean failOnInitialize;
        private boolean failOnClose;

        private RecordingL2Provider(List<String> events) {
            this.events = events;
        }

        @Override
        public void initialize(CacheConfig.L2Config config) {
            events.add("l2.initialize");
            if (failOnInitialize) {
                throw new IllegalStateException("l2 initialize failure");
            }
        }

        @Override
        public CacheConfig.L2ProviderType providerType() {
            return CacheConfig.L2ProviderType.JEDIS;
        }

        @Override
        public String get(CacheKey key) {
            return null;
        }

        @Override
        public void set(CacheKey key, String value, Duration ttl) {
        }

        @Override
        public void delete(CacheKey key) {
        }

        @Override
        public void publish(String channel, String message, L2PubSubMode mode) {
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener, L2PubSubMode mode) {
            return () -> { };
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            return null;
        }

        @Override
        public void close() {
            events.add("l2.close");
            if (failOnClose) {
                throw new IllegalStateException("l2 close failure");
            }
        }
    }

    private static final class RecordingCodec implements CacheCodec, AutoCloseable {
        private final List<String> events;
        private boolean failOnInitialize;

        private RecordingCodec(List<String> events) {
            this.events = events;
        }

        @Override
        public void initialize(CacheConfig config) {
            events.add("codec.initialize");
            if (failOnInitialize) {
                throw new IllegalStateException("codec initialize failure");
            }
        }

        @Override
        public String encode(Object obj) throws CacheCodecException {
            return "";
        }

        @Override
        public <T> T decode(String data, Class<T> clazz) throws CacheCodecException {
            return null;
        }

        @Override
        public <T> CacheMessage<T> decodeMessage(String data, Class<T> dataClass) throws CacheCodecException {
            return null;
        }

        @Override
        public void close() {
            events.add("codec.close");
        }
    }
}
