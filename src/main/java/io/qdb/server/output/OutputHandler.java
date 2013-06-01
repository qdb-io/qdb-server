package io.qdb.server.output;

import java.io.Closeable;
import java.io.IOException;

/**
 * Watches a queue for new messages and does something with them.
 */
public class OutputHandler implements Closeable {

    public OutputHandler() {
    }

    @Override
    public void close() throws IOException {
    }
}
