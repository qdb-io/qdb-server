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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Extend this instead of implementing {@link OutputHandler} directly as new methods may be added to the interface
 * in future.
 */
public abstract class OutputHandlerAdapter implements OutputHandler {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void init(Queue q, Output output, String outputPath) throws Exception {
    }

    @Override
    public void updateOutput(Output output) {
    }

    @Override
    public void close() throws IOException {
    }
}
