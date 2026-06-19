package io.github.dk900912.multitiercache.api;

/**
 * Listener for receiving cache messages from a Pub/Sub channel.
 *
 * @author dukui
 */
@FunctionalInterface
public interface CacheMessageListener {

    /**
     * Invoked when a message is received.
     *
     * @param channel the channel from which the message was received
     * @param message the message payload
     */
    void onMessage(String channel, String message);

    /**
     * Invoked when the provider detects that Pub/Sub delivery became unreliable or recovered.
     * The default implementation preserves lambda and existing listener compatibility.
     */
    default void onDeliveryEvent(CacheMessageDeliveryEvent event) {
    }
}
