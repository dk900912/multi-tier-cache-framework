package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageRepository;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.CacheMutationProcessor;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;
import io.github.dk900912.multitiercache.codec.JacksonCacheCodec;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CacheMessageReplayerTest {

    @Test
    void shouldTreatNullBatchAsEmpty() throws Exception {
        RecordingRepository repository = new RecordingRepository();
        repository.messages = null;
        RecordingMutationProcessor processor = new RecordingMutationProcessor();
        CacheMessageReplayer replayer = createReplayer(repository, processor);

        assertDoesNotThrow(() -> invokeCompensate(replayer));
        assertEquals(0, processor.appliedMessages.size());
    }

    @Test
    void shouldSkipNullEntriesAndProcessValidMessages() throws Exception {
        RecordingRepository repository = new RecordingRepository();
        CacheMessage<String> update = new CacheMessage<>("user:1", "value", 1L, CacheMessageType.UPDATE, 1000L);
        repository.messages = Arrays.asList(null, update);
        RecordingMutationProcessor processor = new RecordingMutationProcessor();
        CacheMessageReplayer replayer = createReplayer(repository, processor);

        invokeCompensate(replayer);

        assertEquals(List.of(update), processor.appliedMessages);
        assertEquals(List.of("user:1:1"), repository.processedKeys);
    }

    @Test
    void shouldReplaySavedMutationAfterTemporaryL2Failure() throws Exception {
        CacheConfig config = new CacheConfig();
        config.getL1().setEnabled(false);
        config.getCodec().setTrustedPackages(List.of("io.github.dk900912"));

        InMemoryRepository repository = new InMemoryRepository();
        FlakyL2Provider l2Provider = new FlakyL2Provider();
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        CacheRuntimeMetricsRecorder runtimeMetrics = new CacheRuntimeMetricsRecorder();

        DefaultCacheManager cacheManager = new DefaultCacheManager(
                config,
                new NoopL1Provider(),
                l2Provider,
                repository,
                codec,
                new SingleFlight(),
                runtimeMetrics
        );

        CacheKey key = CacheKey.simple("user:compensate");
        cacheManager.insert(key, "value", 1L, java.time.Duration.ofMinutes(1));

        assertEquals(1, repository.messages.size(), "Failed mutation should be saved for compensation");
        CacheMessage<?> savedMessage = repository.messages.getFirst();
        assertEquals(1L, savedMessage.getVersion());

        CacheMessageReplayer replayer = createReplayer(repository, cacheManager, runtimeMetrics);
        invokeCompensate(replayer);

        assertNotNull(l2Provider.latestPayload, "Replay should eventually write payload to L2");
        CacheMessage<?> replayedMessage = codec.decodeMessage(l2Provider.latestPayload, Object.class);
        assertEquals(1L, replayedMessage.getVersion());
        assertEquals(List.of("user:compensate:1"), repository.processedKeys);

        CacheRuntimeStats stats = cacheManager.getMonitor().getRuntimeStats();
        assertEquals(1L, stats.getCompensationSaveSuccesses());
        assertEquals(1L, stats.getReplayRuns());
        assertEquals(1L, stats.getReplayMessagesFetched());
        assertEquals(1L, stats.getReplayMessagesApplied());
        assertEquals(0L, stats.getReplayMessagesFailed());
    }

    private static CacheMessageReplayer createReplayer(CacheMessageRepository repository,
                                                       CacheMutationProcessor processor) {
        return createReplayer(repository, processor, new CacheRuntimeMetricsRecorder());
    }

    private static CacheMessageReplayer createReplayer(CacheMessageRepository repository,
                                                       CacheMutationProcessor processor,
                                                       CacheRuntimeMetricsRecorder runtimeMetrics) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        return new CacheMessageReplayer(repository, processor, new CacheConfig(), scheduler, runtimeMetrics);
    }

    private static void invokeCompensate(CacheMessageReplayer replayer) throws Exception {
        Method method = CacheMessageReplayer.class.getDeclaredMethod("compensate");
        method.setAccessible(true);
        try {
            method.invoke(replayer);
        } finally {
            replayer.shutdown();
        }
    }

    private static final class RecordingRepository implements CacheMessageRepository {
        private List<CacheMessage<?>> messages = List.of();
        private final List<String> processedKeys = new ArrayList<>();

        @Override
        public void save(CacheMessage<?> message) {
        }

        @Override
        public List<CacheMessage<?>> fetchUnprocessed(int limit) {
            return messages;
        }

        @Override
        public void markProcessed(String key, Long version) {
            processedKeys.add(key + ":" + version);
        }
    }

    private static final class InMemoryRepository implements CacheMessageRepository {
        private final List<CacheMessage<?>> messages = new ArrayList<>();
        private final List<String> processedKeys = new ArrayList<>();

        @Override
        public void save(CacheMessage<?> message) {
            messages.add(message);
        }

        @Override
        public List<CacheMessage<?>> fetchUnprocessed(int limit) {
            return new ArrayList<>(messages);
        }

        @Override
        public void markProcessed(String key, Long version) {
            processedKeys.add(key + ":" + version);
        }
    }

    private static final class NoopL1Provider implements L1Provider {
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
    }

    private static final class FlakyL2Provider implements L2Provider {
        private boolean firstApply = true;
        private String latestPayload;

        @Override
        public String get(CacheKey key) {
            return latestPayload;
        }

        @Override
        public void set(CacheKey key, String value, java.time.Duration ttl) {
            latestPayload = value;
        }

        @Override
        public void delete(CacheKey key) {
        }

        @Override
        public void publish(String channel, String message) {
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, io.github.dk900912.multitiercache.api.CacheMessageListener listener) {
            return () -> {};
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            if (CacheLuaScripts.APPLY_MESSAGE_LUA_SCRIPT.equals(script)) {
                if (firstApply) {
                    firstApply = false;
                    throw new IllegalStateException("Simulated transient L2 failure");
                }
                latestPayload = args.getFirst();
                return 1L;
            }
            throw new IllegalArgumentException("Unexpected script");
        }
    }

    private static final class RecordingMutationProcessor implements CacheMutationProcessor {
        private final List<CacheMessage<?>> appliedMessages = new ArrayList<>();

        @Override
        public void apply(CacheMessage<?> message) {
            appliedMessages.add(message);
        }
    }
}
