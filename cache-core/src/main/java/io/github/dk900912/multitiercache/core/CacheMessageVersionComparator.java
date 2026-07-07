package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheMessage;

/**
 * Utility class for comparing cache message versions.
 * <p>
 * Provides methods to extract version numbers from cached values and determine
 * whether a cache entry should be updated based on version comparison rules.
 * </p>
 *
 * @author dukui
 */
public final class CacheMessageVersionComparator {

    private CacheMessageVersionComparator() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Determines whether an incoming cache message should replace the current one.
     */
    public static boolean shouldReplace(CacheMessage<?> incoming, CacheMessage<?> current) {
        if (incoming == null) {
            return false;
        }
        if (current == null) {
            return true;
        }

        if (incoming.getType() == null || current.getType() == null) {
            return false;
        }

        if (incoming.getType().isPenetration()) {
            return current.getType().isPenetration();
        }

        if (current.getType().isPenetration()) {
            return true;
        }

        if (incoming.getVersion() == null) {
            return false;
        }
        if (current.getVersion() == null) {
            return true;
        }

        if (incoming.getType().isDelete()) {
            if (current.getType().isDelete()) {
                return incoming.getVersion() > current.getVersion();
            }
            return incoming.getVersion() >= current.getVersion();
        }
        if (current.getType().isDelete()) {
            return false;
        }

        return incoming.getVersion() > current.getVersion();
    }

    /**
     * Determines whether an incoming mutation should invalidate the local L1 entry.
     */
    public static boolean shouldInvalidate(CacheMessage<?> local, CacheMessage<?> incoming) {
        return shouldReplace(incoming, local);
    }

}
