package io.qdb.server.output;

import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;

import java.io.IOException;

/**
 * Extend this instead of implementing {@link OutputHandler} directly as new methods may be added to the interface
 * in future.
 */
public abstract class OutputHandlerAdapter implements OutputHandler {

    @Override
    public void init(Queue q, Output output) {
    }

    @Override
    public void updateOutput(Output output) {
    }

    @Override
    public void close() throws IOException {
    }
}
