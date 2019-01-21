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

import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ClientBuilderImpl;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Published messages to a Apache Pulsar cluster.
 */
public class PulsarOutputHandler extends OutputHandlerAdapter {

    public String topic;

    private String outputPath;
    private Producer<byte[]> producer;

    private Deque<Long> outstandingMessages = new LinkedList<Long>(); // QDB messageId's for messages being sent async
    private volatile long oldestQdbMessageIdAckedByPulsar;
    private volatile Exception pulsarSendError;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Queue q, Output output, String outputPath) throws Exception {
        this.outputPath = outputPath;

        if (output.getUrl() == null) throw new IllegalArgumentException("url is required");

        if (log.isDebugEnabled()) log.debug(outputPath + ": init");

        PulsarClient client = new ClientBuilderImpl().serviceUrl(output.getUrl()).build();
        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .producerName("qdb@" + InetAddress.getLocalHost().getHostName());

        producer = producerBuilder.topic(topic)
                .maxPendingMessages(100)
                .blockIfQueueFull(true)
                .sendTimeout(10, TimeUnit.SECONDS)
                .compressionType(CompressionType.LZ4)
                .create();
    }

    @Override
    public long processMessage(final long qdbMessageId, String routingKey, long timestamp, byte[] payload) throws Exception {
        if (log.isDebugEnabled()) log.debug(outputPath + ": Publishing " + qdbMessageId);
        if (pulsarSendError != null) throw pulsarSendError;
        synchronized (this) {
            outstandingMessages.addLast(qdbMessageId);
        }
        producer.sendAsync(payload).handle(new BiFunction<MessageId, Throwable, Object>() {
            public Object apply(MessageId messageId, Throwable t) {
                if (t == null) {
                    PulsarOutputHandler.this.onMessageAckedByPulsar(qdbMessageId);
                } else {
                    pulsarSendError = t instanceof Exception ? (Exception) t : new Exception(t);
                    if (log.isDebugEnabled()) log.debug(outputPath + ": Send failed: " + pulsarSendError);
                }
                return null;
            }
        });
        if (log.isDebugEnabled()) log.debug(outputPath + ": Publishing return " + oldestQdbMessageIdAckedByPulsar);
        return oldestQdbMessageIdAckedByPulsar;
    }

    private synchronized void onMessageAckedByPulsar(Long qdbMessageId) {
        if (qdbMessageId.equals(outstandingMessages.peekFirst())) {
            if (log.isDebugEnabled()) log.debug(outputPath + ": Acked first " + qdbMessageId);
            outstandingMessages.removeFirst();
            oldestQdbMessageIdAckedByPulsar = qdbMessageId;
        } else {
            if (log.isDebugEnabled()) log.debug(outputPath + ": Acked " + qdbMessageId);
            outstandingMessages.removeFirstOccurrence(qdbMessageId);
        }
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            if (log.isDebugEnabled()) log.debug(outputPath + ": Closing producer " + producer);
            producer.close();
            producer = null;
        }
    }
}
