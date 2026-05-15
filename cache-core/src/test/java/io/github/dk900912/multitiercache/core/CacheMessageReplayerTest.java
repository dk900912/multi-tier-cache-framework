package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheMessageRepository;
import io.github.dk900912.multitiercache.api.CacheMutationProcessor;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

    private static CacheMessageReplayer createReplayer(RecordingRepository repository,
                                                       RecordingMutationProcessor processor) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        return new CacheMessageReplayer(repository, processor, new CacheConfig(), scheduler);
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

    private static final class RecordingMutationProcessor implements CacheMutationProcessor {
        private final List<CacheMessage<?>> appliedMessages = new ArrayList<>();

        @Override
        public void apply(CacheMessage<?> message) {
            appliedMessages.add(message);
        }
    }
}
