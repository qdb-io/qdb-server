package io.qdb.server.output;

import com.rabbitmq.client.*;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;

import java.io.IOException;

/**
 * Published messages to a RabbitMQ server.
 */
public class RabbitMQOutputHandler extends OutputHandlerAdapter implements ShutdownListener {

    public String exchange;
    public String[] queues;
    public int heartbeat = 30;

    protected Output output;
    protected String outputPath;
    protected ConnectionFactory connectionFactory;
    protected Connection con;
    protected Channel channel;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Queue q, Output output, String outputPath) throws Exception {
        this.output = output;
        this.outputPath = outputPath;

        if (output.getUrl() == null) throw new IllegalArgumentException("url is required");
        if (exchange == null) throw new IllegalArgumentException("exchange is required");

        connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(output.getUrl());
        connectionFactory.setRequestedHeartbeat(heartbeat);
        ensureChannel();
    }

    @Override
    public long processMessage(long messageId, String routingKey, long timestamp, byte[] payload) throws Exception {
        if (log.isDebugEnabled()) log.debug(outputPath + ": Publishing " + messageId);
        ensureChannel().basicPublish(exchange, routingKey, null, payload);
        return messageId;
    }

    @Override
    public void close() throws IOException {
        // this method cannot be synchronized or we get deadlock with shutdownCompleted
        if (con != null) con.close();
    }

    protected synchronized Channel ensureChannel() throws Exception {
        if (channel == null) {
            con = connectionFactory.newConnection();
            channel = con.createChannel();
            if (log.isInfoEnabled()) log.info(outputPath + ": Connected to " + getConnectionInfo());
            channel.addShutdownListener(this);
            initChannel(channel);
        }
        return channel;
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

    @Override
    public synchronized void shutdownCompleted(ShutdownSignalException cause) {
        channel = null;
        if (!cause.isInitiatedByApplication()) {
            log.error(outputPath + ": Channel closed unexpectedly: " + cause.getMessage());
            try {
                if (con.isOpen()) con.close();
            } catch (Exception ignore) {
            }
            con = null;
        }
    }

    protected String getConnectionInfo() {
        if (connectionFactory == null) return "(null)";
        String host = connectionFactory.getHost();
        if (host.length() == 0) host="127.0.0.1";
        return "amqp" + (connectionFactory.isSSL() ? "s" : "") + "://" + connectionFactory.getUsername() +
                "@" + host + ":" + connectionFactory.getPort() + connectionFactory.getVirtualHost();
    }
}
