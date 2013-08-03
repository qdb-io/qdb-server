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

import io.qdb.server.model.Input;
import io.qdb.server.model.Queue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * Fetches messages from somewhere to be appended to a queue. Public fields are automatically populated with
 * parameters from the input before message processing starts.
 */
public interface InputHandler extends Closeable {

    interface Sink {
        void append(String routingKey, byte[] payload) throws IOException;
        void append(String routingKey, ReadableByteChannel payload, int payloadSize) throws IOException;
    }

    /**
     * This is called once before the first call to {@link #fetchMessages(io.qdb.server.input.InputHandler.Sink, int)}
     * Note that the input instance will become stale i.e. once processing has started it will no longer reflect the current
     * state of the input. Throw IllegalArgumentException for permanent errors (e.g. missing or invalid parameters)
     * which will cause the input to stop until it is updated. Throwing other exceptions will cause the input
     * to be retried after a delay defined by its backoff policy.
     */
    void init(Queue q, Input input, String inputPath) throws Exception;

    /**
     * Fetch messages until this thread is interrupted. Pass each message to one of the append methods on the sink.
     */
    void fetchMessages(Sink sink) throws Exception;

    /**
     * This is called when processing progress is being recorded with the new input instance. Handlers might want
     * to update fields of the input at this time. This method must be fast.
     */
    void updateInput(Input input);

}
