package io.github.dk900912.multitiercache.api;

/**
 * Delivery state changes reported by an L2 Pub/Sub provider.
 *
 * @author dukui
 */
public enum CacheMessageDeliveryEventType {
    PROCESSING_OVERLOADED,
    PROCESSING_RECOVERED,
    SUBSCRIPTION_INTERRUPTED,
    SUBSCRIPTION_RESTORED
}
