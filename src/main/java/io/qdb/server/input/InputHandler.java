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
 * Fetches messages from somewhere and appends them to a queue. Public fields are automatically populated with
 * parameters from the input before message processing starts.
 */
public interface InputHandler extends Closeable {

    /**
     * The append methods throw an IllegalArgumentException if the message exceeds the queue maxPayloadSize.
     */
    interface Sink {

        /** Append a message, throws an IllegalArgumentException if payload exceeds the queue maxPayloadSize. */
        void append(String routingKey, byte[] payload) throws IOException, IllegalArgumentException;

        /** Append a message, throws an IllegalArgumentException if payload exceeds the queue maxPayloadSize. */
        void append(String routingKey, ReadableByteChannel payload, int payloadSize) throws IOException, IllegalArgumentException;

        /** Record an error. Call this on errors communicating with the source of the messages etc. */
        void error(String msg, Throwable t);

        /** Record an error. Call this on errors communicating with the source of the messages etc. */
        void error(Throwable t);
    }

    /**
     * This is called once before the first call to {@link #start }.
     * Note that the input instance will become stale i.e. once processing has started it will no longer reflect the current
     * state of the input. Throw IllegalArgumentException for permanent errors (e.g. missing or invalid parameters)
     * which will cause the input to stop until it is updated. Throwing other exceptions will cause the input
     * to be retried after a delay defined by its backoff policy.
     */
    void init(Queue q, Input input, String inputPath) throws Exception;

    /**
     * Fetch messages until {@link #close()} is called. Pass each message to one of the append methods
     * on the sink. This method can block or return immediately and append messages asynchronously.
     * Note that close() may be called before the first call to this method and may also be called more than once.
     */
    void start(Sink sink) throws Exception;

    /**
     * This is called when processing progress is being recorded with the new input instance. Handlers might want
     * to update fields of the input at this time. This method must be fast.
     */
    void updateInput(Input input);

}
