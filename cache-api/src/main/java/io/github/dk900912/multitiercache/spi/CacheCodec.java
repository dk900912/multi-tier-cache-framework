package io.github.dk900912.multitiercache.spi;

import io.github.dk900912.multitiercache.api.exception.CacheCodecException;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;

/**
 * Service Provider Interface for serializing and deserializing cache payloads and messages.
 */
public interface CacheCodec {

    default void initialize(CacheConfig config) {}

    String encode(Object obj) throws CacheCodecException;

    <T> T decode(String data, Class<T> clazz) throws CacheCodecException;

    <T> CacheMessage<T> decodeMessage(String data, Class<T> dataClass) throws CacheCodecException;
}
