package io.github.dk900912.multitiercache.api.model;

/**
 * Enumerates the types of cache mutations and messages.
 *
 * @author dukui
 */
public enum CacheMessageType {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete"),
    PENETRATE("penetrate"),
    BACKFILL("backfill");

    private final String wireValue;

    CacheMessageType(String wireValue) {
        this.wireValue = wireValue;
    }

    public String getWireValue() {
        return wireValue;
    }
}
