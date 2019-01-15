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
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ClientBuilderImpl;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Published messages to a Apache Pulsar cluster.
 */
public class PulsarOutputHandler extends OutputHandlerAdapter {

    public String topic;

    protected String outputPath;
    protected Producer<byte[]> producer;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Queue q, Output output, String outputPath) throws Exception {
        this.outputPath = outputPath;

        if (output.getUrl() == null) throw new IllegalArgumentException("url is required");

        PulsarClient client = new ClientBuilderImpl().serviceUrl(output.getUrl()).build();
        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .producerName("qdb@" + InetAddress.getLocalHost().getHostName());

        producer = producerBuilder.topic(topic).maxPendingMessages(10000).blockIfQueueFull(true)
                .compressionType(CompressionType.LZ4)
                .create();
    }

    @Override
    public long processMessage(long messageId, String routingKey, long timestamp, byte[] payload) throws Exception {
        if (log.isDebugEnabled()) log.debug(outputPath + ": Publishing " + messageId);
        producer.send(payload);
        return messageId;
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }
}
