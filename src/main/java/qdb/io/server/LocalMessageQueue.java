package qdb.io.server;

import qdb.io.buffer.MessageBuffer;
import qdb.io.buffer.PersistentMessageBuffer;

import java.io.File;
import java.io.IOException;

/**
 * Message queue stored on this node. We may be the master or a slave replicating the data from another node.
 */
public class LocalMessageQueue extends MessageQueue {

    private final PersistentMessageBuffer mb;

    public LocalMessageQueue(String id, File dir, long firstMessageId) throws IOException {
        super(id);
        mb = new PersistentMessageBuffer(dir, firstMessageId);
    }

    public MessageBuffer getMessageBuffer() {
        return mb;
    }

    @Override
    public void close() throws IOException {
        mb.close();
    }
}
