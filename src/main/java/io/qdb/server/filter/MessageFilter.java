package io.qdb.server.filter;

/**
 * For filtering messages retuned by the message list endpoint and for outputs etc.
 */
public interface MessageFilter {

    /**
     * This method is called before the first call to {@link #accept(long, String, byte[])}.
     */
    void init() throws IllegalArgumentException;

    /**
     * Return true to process (accept) the message or false to skip it. This method is called twice for each message,
     * once with just the timestamp and routingKey and once with these and the message payload.
     */
    boolean accept(long timestamp, String routingKey, byte[] payload);
}
