package io.qdb.server.output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pushes messages to a RabbitMQ server.
 */
public class RabbitMQOutputHandler extends OutputHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQOutputHandler.class);

    @Override
    public long processMessage(long messageId, String routingKey, long timestamp, byte[] payload) {
        log.debug("processMessage " + messageId);
        return messageId;
    }
}
