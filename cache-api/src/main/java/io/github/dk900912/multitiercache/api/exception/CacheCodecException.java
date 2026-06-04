package io.github.dk900912.multitiercache.api.exception;

public class CacheCodecException extends RuntimeException {
    public CacheCodecException(String message) {
        super(message);
    }

    public CacheCodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
