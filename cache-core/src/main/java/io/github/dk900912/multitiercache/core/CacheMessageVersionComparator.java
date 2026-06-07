package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for comparing cache message generations and versions.
 * <p>
 * Provides methods to extract version numbers from cached values and determine
 * whether a cache entry should be updated based on version comparison rules.
 * </p>
 *
 * @author dukui
 */
public final class CacheMessageVersionComparator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheMessageVersionComparator.class);

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

        int generationComparison = compareNullableLong(incoming.getGeneration(), current.getGeneration());
        if (generationComparison != 0) {
            return generationComparison > 0;
        }

        if (incoming.getVersion() == null) {
            return false;
        }
        if (current.getVersion() == null) {
            return true;
        }

        if (incoming.getType().isDelete()) {
            return incoming.getVersion() >= current.getVersion();
        }
        return incoming.getVersion() > current.getVersion();
    }

    /**
     * Determines whether an incoming mutation should invalidate the local L1 entry.
     */
    public static boolean shouldInvalidate(CacheMessage<?> local, CacheMessage<?> incoming) {
        return shouldReplace(incoming, local);
    }

    private static int compareNullableLong(Long left, Long right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        return Long.compare(left, right);
    }
}
