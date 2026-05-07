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
        LOGGER.warn("Cache mutation failed and no custom LocalEventStore is configured. message={}", message);
    }

    @Override
    public List<CacheMessage<?>> fetchUnprocessed(int limit) {
        return Collections.emptyList();
    }

    @Override
    public void markProcessed(String key, Long version) {
        LOGGER.debug("Ignoring processed mark in default LocalEventStore. key={}, version={}", key, version);
    }
}
