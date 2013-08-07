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

import java.io.Closeable;

/**
 * Processes messages from a queue. Public fields are automatically populated with parameters from the output
 * before message processing starts.
 */
public interface OutputHandler extends Closeable {

    /**
     * This is called once before the first call to {@link #processMessage(long, String, long, byte[])}. Note that
     * the output instance will become stale i.e. once processing has started it will no longer reflect the current
     * state of the output. Throw IllegalArgumentException for permanent errors (e.g. missing or invalid parameters)
     * which will cause the output to stop until it is updated. Throwing other exceptions will cause the output
     * to be retried after a delay defined by its backoff policy.
     */
    void init(Queue q, Output output, String outputPath) throws Exception;

    /**
     * Process the message and return the id of the message that processing should start after. This will usually
     * be the id of the message just processed, unless messages are being processed asynchronously.
     */
    long processMessage(long messageId, String routingKey, long timestamp, byte[] payload) throws Exception;

    /**
     * Finish processing any messages being processed asynchronously and return the id of the message that processing
     * should start after. This method is called when an output reaches its 'to' or 'toId' setting before it
     * disables itself. Return 0 if messages are not being processed asynchronously.
     */
    long flushMessages() throws Exception;

    /**
     * This is called when processing progress is being recorded with the new output instance. Handlers might want
     * to update fields of the output at this time. In particular the timestamp field will be set to
     * the timestamp of the last message processed and this may not be the same as the last completed message if
     * messages are being processed asynchronously. This method must be fast.
     */
    void updateOutput(Output output);

}
