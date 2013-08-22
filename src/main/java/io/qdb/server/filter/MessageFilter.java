package io.qdb.server.filter;

import io.qdb.server.model.Queue;

/**
 * For filtering messages returned by the message list endpoint and for outputs etc.
 */
public interface MessageFilter {

    enum Result { REJECT, ACCEPT, CHECK_PAYLOAD }

    /**
     * This method is called before the first call to {@link #accept(long, String, byte[])}.
     */
    void init(Queue q) throws IllegalArgumentException;

    /**
     * Return {@link Result#ACCEPT} to process the message or {@link Result#REJECT} to skip it. The first call to
     * this method for a message is made before the payload is read. Return {@link Result#CHECK_PAYLOAD}
     * if the payload is required and a second call will be made once the payload has been read.
     */
    Result accept(long timestamp, String routingKey, byte[] payload);

    /**
     * Accepts all messages.
     */
    public static final MessageFilter NULL = new MessageFilter() {
        public void init(Queue q) throws IllegalArgumentException { }
        public Result accept(long timestamp, String routingKey, byte[] payload) { return Result.ACCEPT; }
    };
}
