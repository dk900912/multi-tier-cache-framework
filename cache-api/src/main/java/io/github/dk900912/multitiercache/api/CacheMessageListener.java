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
}
