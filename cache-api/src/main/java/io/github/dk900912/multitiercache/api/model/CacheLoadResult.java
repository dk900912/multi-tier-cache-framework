package io.github.dk900912.multitiercache.api.model;

import java.time.Duration;
import java.util.Objects;

/**
 * Represents the result of loading a value into the cache.
 * <p>
 * Encapsulates the loaded data, its version, TTL, and whether it indicates a cache penetration (i.e., data not found).
 * Use {@link #of(Object, Long, Duration)} for present data and {@link #penetration(Duration)} for absent data.
 * </p>
 *
 * @param <T> the type of the loaded data
 * @author dukui
 */
public class CacheLoadResult<T> {
    private final T data;
    private final Long version;
    private final Duration ttl;

    private CacheLoadResult(T data, Long version, Duration ttl) {
        this.data = data;
        this.version = version;
        this.ttl = ttl;
    }

    public static <T> CacheLoadResult<T> of(T data, Long version, Duration ttl) {
        return new CacheLoadResult<>(
                Objects.requireNonNull(data, "Data cannot be null"),
                Objects.requireNonNull(version, "Version cannot be null"),
                Objects.requireNonNull(ttl, "TTL cannot be null")
        );
    }

    /**
     * Creates a load result representing cache penetration, meaning the loader confirmed that no data exists.
     *
     * @param ttl the tombstone TTL to store for the penetration marker
     * @param <T> the type of the cached data
     * @return a load result representing an absent value
     */
    public static <T> CacheLoadResult<T> penetration(Duration ttl) {
        return new CacheLoadResult<>(null, -1L, ttl);
    }

    public T getData() {
        return data;
    }

    public Long getVersion() {
        return version;
    }

    public Duration getTtl() {
        return ttl;
    }

    public boolean isPenetration() {
        return data == null;
    }

    public boolean hasVersion() {
        return version != null;
    }
}
