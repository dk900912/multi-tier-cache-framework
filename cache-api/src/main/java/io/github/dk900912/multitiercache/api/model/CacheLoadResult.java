package io.github.dk900912.multitiercache.api.model;

import java.time.Duration;
import java.util.Objects;

/**
 * Represents the result of loading a value into the cache.
 * <p>
 * Encapsulates the loaded data, its version, TTL, and whether it indicates a cache penetration (i.e., data not found).
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

    // todo: ttl必须是mills
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
