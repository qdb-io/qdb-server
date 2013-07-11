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

package io.qdb.server.output;

import com.rabbitmq.client.*;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;

import java.io.IOException;
import java.util.Arrays;

/**
 * Published messages to a RabbitMQ server.
 */
public class RabbitMQOutputHandler extends OutputHandlerAdapter implements ShutdownListener {

    public String exchange;
    public String[] queues;
    public int heartbeat = 30;

    protected String outputPath;
    protected ConnectionFactory connectionFactory;
    protected Connection con;
    protected Channel channel;

    protected String exchangeType;
    protected boolean exchangeDurable;
    protected boolean[] queueDurable;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Queue q, Output output, String outputPath) throws Exception {
        this.outputPath = outputPath;

        if (output.getUrl() == null) throw new IllegalArgumentException("url is required");

        if (queues != null && queues.length == 0) queues = null;
        if (exchange != null && exchange.length() == 0) exchange = null;

        if (queues == null) {
            if (exchange == null) throw new IllegalArgumentException("queues or exchange is required");
        } else {
            queueDurable = new boolean[queues.length];
            for (int i = 0; i < queues.length; i++) {
                String[] toks = queues[i].split("[\\s]*#[\\s]*");
                queues[i] = toks[0];
                queueDurable[i] = toks.length == 1 || "true".equals(toks[1]);
                if (queues[i].length() == 0) throw new IllegalArgumentException("empty queue name");
            }
            if (exchange == null) {
                if (queues.length > 1) throw new IllegalArgumentException("exchange is required for multiple queues");
                exchange = queues[0];
            }
        }

        String[] toks = exchange.split("[\\s]*#[\\s]*");
        exchange = toks[0];
        exchangeType = toks.length >= 2 ? toks[1] : "fanout";
        exchangeDurable = toks.length < 3 || "true".equals(toks[2]);

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
        channel.exchangeDeclare(exchange, exchangeType, exchangeDurable);
        if (queues != null) {
            for (int i = 0; i < queues.length; i++) {
                String q = queues[i];
                channel.queueDeclare(q, queueDurable[i], false, false, null);
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
