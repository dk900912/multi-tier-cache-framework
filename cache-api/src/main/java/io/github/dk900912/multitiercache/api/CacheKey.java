package io.github.dk900912.multitiercache.api;

import java.util.StringJoiner;

/**
 * A functional interface for representing cache keys.
 * <p>
 * Converts the domain-specific key into a Redis-compatible string format.
 * </p>
 *
 * @author dukui
 */
@FunctionalInterface
public interface CacheKey {

    /**
     * Converts this cache key to a string representation suitable for Redis.
     *
     * @return the string representation of the cache key
     */
    String toKeyString();

    public static CacheKey simple(String key) {
        return () -> key;
    }

    public static CacheKey of(String prefix, Object... args) {
        return () -> {
            if (args == null || args.length == 0) {
                return prefix;
            }
            StringJoiner joiner = new StringJoiner(":");
            joiner.add(prefix);
            for (Object arg : args) {
                joiner.add(arg == null ? "null" : String.valueOf(arg));
            }
            return joiner.toString();
        };
    }
}
