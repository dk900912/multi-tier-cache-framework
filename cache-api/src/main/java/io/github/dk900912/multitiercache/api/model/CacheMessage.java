package io.github.dk900912.multitiercache.api.model;

/**
 * Represents a cache message exchanged between nodes, typically over Pub/Sub.
 * <p>
 * Contains the cache key, payload data, version, type of mutation, and TTL.
 * </p>
 *
 * @param <T> the type of the cached payload
 * @author dukui
 */
public final class CacheMessage<T> {
    private String key;
    private T data;
    private Long version;
    private CacheMessageType type;
    private Long ttlMillis;

    public CacheMessage() {}

    public CacheMessage(String key, T data, Long version, CacheMessageType type, Long ttlMillis) {
        this.key = key;
        this.data = data;
        this.version = version;
        this.type = type;
        this.ttlMillis = ttlMillis;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public CacheMessageType getType() {
        return type;
    }

    public void setType(CacheMessageType type) {
        this.type = type;
    }

    public Long getTtlMillis() {
        return ttlMillis;
    }

    public void setTtlMillis(Long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }
}
