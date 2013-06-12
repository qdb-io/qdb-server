package io.qdb.server;

import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.output.OutputHandler;

import java.io.IOException;

/**
 * For testing output stuff.
 */
public class TestOutputHandler implements OutputHandler {

    @Override
    public void init(Queue q, Output output, String outputPath) {
    }

    @Override
    public long processMessage(long messageId, String routingKey, long timestamp, byte[] payload) throws Exception {
        return 0;
    }

    @Override
    public void updateOutput(Output output) {
    }

    @Override
    public void close() throws IOException {
    }
}
