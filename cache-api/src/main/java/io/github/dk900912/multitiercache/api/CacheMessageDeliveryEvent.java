package io.github.dk900912.multitiercache.api;

import java.util.Objects;

/**
 * Describes a detected Pub/Sub delivery gap or recovery.
 *
 * @param channel the affected channel
 * @param type the delivery state change
 * @param episodeId provider-local overload episode identifier, otherwise zero
 * @param droppedMessages messages dropped during the overload episode, otherwise zero
 * @param cause the triggering failure, when available
 *
 * @author dukui
 */
public record CacheMessageDeliveryEvent(
        String channel,
        CacheMessageDeliveryEventType type,
        long episodeId,
        long droppedMessages,
        Throwable cause) {

    public CacheMessageDeliveryEvent(
            String channel,
            CacheMessageDeliveryEventType type,
            long droppedMessages,
            Throwable cause) {
        this(channel, type, 0L, droppedMessages, cause);
    }

    public CacheMessageDeliveryEvent {
        Objects.requireNonNull(channel, "Delivery event channel cannot be null");
        Objects.requireNonNull(type, "Delivery event type cannot be null");
        if (episodeId < 0) {
            throw new IllegalArgumentException("Delivery episode ID cannot be negative");
        }
        if (droppedMessages < 0) {
            throw new IllegalArgumentException("Dropped message count cannot be negative");
        }
    }
}
