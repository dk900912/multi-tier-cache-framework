package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheMessageRepository;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Default, no-op implementation of the {@link io.github.dk900912.multitiercache.api.CacheMessageRepository}.
 * <p>
 * Logs message events but does not persist them.
 * </p>
 *
 * @author dukui
 */
public class DefaultCacheMessageRepository implements CacheMessageRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCacheMessageRepository.class);

    @Override
    public void save(CacheMessage<?> message) {
        LOGGER.info("Skipping cache message persistence because no custom CacheMessageRepository is configured. message={}", message);
    }

    @Override
    public List<CacheMessage<?>> fetchUnprocessed(int limit) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Fetching unprocessed messages ignored. Always returns empty list in default implementation. limit={}", limit);
        }
        return Collections.emptyList();
    }

    @Override
    public void markProcessed(String key, Long generation, Long version) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Ignore marking message as processed in default repository. [key={}, generation={}, version={}]",
                    key, generation, version
            );
        }
    }
}
