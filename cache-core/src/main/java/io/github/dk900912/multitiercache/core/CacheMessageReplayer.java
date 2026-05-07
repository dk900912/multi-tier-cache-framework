package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheMessageRepository;
import io.github.dk900912.multitiercache.api.CacheMutationProcessor;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import io.github.dk900912.multitiercache.api.LifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Replays failed or unprocessed cache messages.
 * <p>
 * Runs as a background task to ensure eventual consistency across the cache tiers.
 * </p>
 *
 * @author dukui
 */
public final class CacheMessageReplayer implements LifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheMessageReplayer.class);

    private final CacheMessageRepository cacheMessageRepository;
    private final CacheMutationProcessor mutationProcessor;
    private final CacheConfig cacheConfig;
    private final ScheduledExecutorService scheduler;

    public CacheMessageReplayer(CacheMessageRepository cacheMessageRepository,
                                CacheMutationProcessor mutationProcessor,
                                CacheConfig cacheConfig,
                                ScheduledExecutorService scheduler) {
        this.cacheMessageRepository = Objects.requireNonNull(cacheMessageRepository, "LocalEventStore cannot be null");
        this.mutationProcessor = Objects.requireNonNull(mutationProcessor, "CacheMutationProcessor cannot be null");
        this.cacheConfig = Objects.requireNonNull(cacheConfig, "CacheConfig cannot be null");
        this.scheduler = Objects.requireNonNull(scheduler, "ScheduledExecutorService cannot be null");
    }

    @Override
    public void bootstrap() {
        scheduler.scheduleAtFixedRate(
                this::compensate,
                cacheConfig.getCompensation().getInitialDelay().toMillis(),
                cacheConfig.getCompensation().getPeriod().toMillis(),
                TimeUnit.MILLISECONDS
        );
        LOGGER.info("Compensation task started.");
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOGGER.info("Compensation task stopped");
    }

    private void compensate() {
        try {
            List<CacheMessage<?>> messages = cacheMessageRepository.fetchUnprocessed(cacheConfig.getCompensation().getBatchSize());
            for (CacheMessage<?> message : messages) {
                if (message == null
                        || message.getType() == CacheMessageType.PENETRATE
                        || message.getType() == CacheMessageType.BACKFILL) {
                    continue;
                }
                try {
                    mutationProcessor.apply(message);
                    cacheMessageRepository.markProcessed(message.getKey(), message.getVersion());
                } catch (Exception e) {
                    LOGGER.error("Failed to compensate cache message for key {}", message.getKey(), e);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to execute compensation task", e);
        }
    }
}
