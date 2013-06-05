package io.qdb.server.output;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Published messages to a RabbitMQ server.
 */
public class RabbitMQOutputHandler extends OutputHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQOutputHandler.class);

    public String exchange;
    public String[] queues;

    private Output output;
    private Channel channel;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Queue q, Output output) {
        this.output = output;
        if (output.getUrl() == null) throw new IllegalArgumentException("url is required");
        if (exchange == null) throw new IllegalArgumentException("exchange is required");
        queues = new String[]{exchange};
    }

    @Override
    public long processMessage(long messageId, String routingKey, long timestamp, byte[] payload) throws Exception {
        log.debug("Processing " + messageId);
        if (channel == null) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setUri(output.getUrl());
            initChannel(channel = cf.newConnection().createChannel());
        }
        channel.basicPublish(exchange, routingKey, null, payload);
        return messageId;
    }

    protected void initChannel(Channel channel) throws IOException {
        channel.exchangeDeclare(exchange, "fanout", true);
        if (queues != null) {
            for (String q : queues) {
                channel.queueDeclare(q, true, false, false, null);
                channel.queueBind(q, exchange, "");
            }
        }
    }
}
