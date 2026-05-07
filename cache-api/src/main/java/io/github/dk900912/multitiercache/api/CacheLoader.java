package io.github.dk900912.multitiercache.api;

import io.github.dk900912.multitiercache.api.model.CacheLoadResult;

/**
 * A loader function for providing values when a cache miss occurs.
 * <p>
 * Returns a comprehensive {@link CacheLoadResult} containing data, version, and TTL.
 * </p>
 *
 * @param <T> the type of the cached data
 * @author dukui
 */
@FunctionalInterface
public interface CacheLoader<T> {

    /**
     * Loads the value and metadata when a cache miss occurs.
     *
     * @return a {@link CacheLoadResult} encapsulating the data, version, and TTL
     */
    CacheLoadResult<T> load();
}
