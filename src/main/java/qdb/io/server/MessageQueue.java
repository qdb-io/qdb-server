package qdb.io.server;

import java.io.Closeable;
import java.io.IOException;

/**
 * Base class for a message queue.
 */
public abstract class MessageQueue implements Closeable {

    protected final String id;
    protected String name;

    protected MessageQueue(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void close() throws IOException {
    }
}
