/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.server.input;

import com.rabbitmq.client.*;
import io.qdb.server.model.Input;
import io.qdb.server.model.Queue;
import io.qdb.server.output.OutputException;

import java.io.IOException;
import java.net.ConnectException;

/**
 * Fetches messages from a RabbitMQ queue.
 */
public class RabbitMQInputHandler extends InputHandlerAdapter implements ShutdownListener {

    public String exchange;
    public String queue;
    public String routingKey = "";
    public int heartbeat = 30;
    public boolean autoAck;

    protected String inputPath;
    protected ConnectionFactory connectionFactory;
    protected Connection con;
    protected Channel channel;

    protected String exchangeType;
    protected boolean exchangeDurable;
    protected boolean queueDurable;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Queue q, Input input, String inputPath) throws Exception {
        super.init(q, input, inputPath);
        this.inputPath = inputPath;

        if (input.getUrl() == null) throw new IllegalArgumentException("url is required");

        if (queue != null && queue.length() == 0) queue = null;
        if (exchange != null && exchange.length() == 0) exchange = null;

        if (queue == null) {
            if (exchange == null) throw new IllegalArgumentException("queue is required");
        } else {
            String[] toks = queue.split("[\\s]*#[\\s]*");
            queue = toks[0];
            queueDurable = toks.length == 1 || "true".equals(toks[1]);
            if (queue.length() == 0) throw new IllegalArgumentException("empty queue name");
        }

        if (exchange != null) {
            String[] toks = exchange.split("[\\s]*#[\\s]*");
            exchange = toks[0];
            exchangeType = toks.length >= 2 ? toks[1] : "fanout";
            exchangeDurable = toks.length < 3 || "true".equals(toks[2]);
        }

        connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(input.getUrl());
        connectionFactory.setRequestedHeartbeat(heartbeat);
        ensureChannel();
    }

    @Override
    public void fetchMessages(final Sink sink) throws Exception {
        final Channel c = ensureChannel();
        c.basicConsume(queue, autoAck, "",
            new DefaultConsumer(c) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    boolean ok = false;
                    try {
                        sink.append(envelope.getRoutingKey(), body);
                        if (!autoAck) c.basicAck(envelope.getDeliveryTag(), false);
                        ok = true;
                    } finally {
                        if (!ok) {
                            try {
                                c.basicNack(envelope.getDeliveryTag(), false, true);
                            } catch (IOException e) {
                                log.debug("Error nacking message: " + e, e);
                            }
                        }
                    }
                }
            }
        );
    }

    @Override
    public void close() throws IOException {
        // this method cannot be synchronized or we get deadlock with shutdownCompleted
        if (con != null) con.close();
    }

    protected synchronized Channel ensureChannel() throws Exception {
        if (channel == null) {
            try {
                con = connectionFactory.newConnection();
            } catch (ConnectException e) {
                throw new OutputException(e.getMessage() + ": " + getConnectionInfo());
            }
            channel = con.createChannel();
            if (log.isInfoEnabled()) log.info(inputPath + ": Connected to " + getConnectionInfo());
            channel.addShutdownListener(this);
            initChannel(channel);
        }
        return channel;
    }

    protected void initChannel(Channel channel) throws IOException {
        channel.queueDeclare(queue, queueDurable, false, false, null);
        if (exchange != null) {
            channel.exchangeDeclare(exchange, exchangeType, exchangeDurable);
            channel.queueBind(queue, exchange, routingKey);
        }
    }

    @Override
    public synchronized void shutdownCompleted(ShutdownSignalException cause) {
        channel = null;
        if (!cause.isInitiatedByApplication()) {
            log.error(inputPath + ": Channel closed unexpectedly: " + cause.getMessage());
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
