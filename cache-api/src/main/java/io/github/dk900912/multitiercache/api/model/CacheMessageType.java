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

    public boolean isDelete() {
        return this == DELETE;
    }

    public boolean isPenetration() {
        return this == PENETRATE;
    }

    public static CacheMessageType fromWireValue(String wireValue) {
        if (wireValue == null) {
            throw new IllegalArgumentException("CacheMessageType cannot be null");
        }
        for (CacheMessageType type : values()) {
            if (type.getWireValue().equalsIgnoreCase(wireValue)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported CacheMessageType: " + wireValue);
    }
}
