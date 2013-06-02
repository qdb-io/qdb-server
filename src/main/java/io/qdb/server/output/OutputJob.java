package io.qdb.server.output;

import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.MessageCursor;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.repo.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Watches a queue for new messages and processes them.
 */
public class OutputJob {

    private static final Logger log = LoggerFactory.getLogger(OutputJob.class);

    private final Repository repo;
    private final String oid;
    private final OutputHandler handler;

    private Thread thread;
    private boolean stopFlag;
    private Output output;

    public OutputJob(Repository repo, String oid, OutputHandler handler) {
        this.repo = repo;
        this.oid = oid;
        this.handler = handler;
    }

    public String getOid() {
        return oid;
    }

    /**
     * Feed messages to our handler until we are closed or our output is changed by someone else.
     */
    public void processMessages(Output o, Queue q, MessageBuffer buffer) throws IOException {
        this.output = o;
        synchronized (this) {
            thread = Thread.currentThread();
            if (stopFlag) return; // someone already stopped us
        }
        MessageCursor cursor = null;
        try {
            handler.init(q, output);

            long messageId = output.getMessageId();
            if (messageId == -2) {
                cursor = buffer.cursorByTimestamp(output.getTimestamp());
            } else {
                cursor = buffer.cursor(messageId < 0 ? buffer.getNextMessageId() : messageId);
            }

            long completedId = messageId;
            long timestamp = 0;
            long lastUpdate = System.currentTimeMillis();
            int updateIntervalMs = output.getUpdateIntervalMs();
            Thread thread = Thread.currentThread();
            while (!stopFlag && !thread.isInterrupted()) {
                boolean haveMsg;
                try {
                    haveMsg = cursor.next(100);
                } catch (IOException e) {
                    haveMsg = false;
                    stopFlag = true;
                    log.error(e.toString(), e);
                } catch (InterruptedException e) {
                    haveMsg = false;
                    stopFlag = true;
                }
                if (haveMsg) {
                    try {
                        completedId = handler.processMessage(cursor.getId(), cursor.getRoutingKey(),
                                timestamp = cursor.getTimestamp(), cursor.getPayload());
                    } catch (Exception e) {
                        stopFlag =  true;
                        log.error(e.toString(), e);
                    }
                }

                o = repo.findOutput(oid);
                if (o != output) stopFlag = true; // output has been changed by someone else

                if (completedId != messageId && (stopFlag || updateIntervalMs <= 0
                        || System.currentTimeMillis() - lastUpdate >= updateIntervalMs)) {
                    synchronized (repo) {
                        o = repo.findOutput(oid);
                        // don't record our progress if we are now supposed to be processing from a different point in buffer
                        if (o.getMessageId() != output.getMessageId() || o.getTimestamp() != output.getTimestamp()) break;
                        output = (Output)o.clone();
                        output.setTimestamp(timestamp);
                        handler.updateOutput(output);
                        output.setMessageId(completedId + 1); // +1 so we don't repeat the same message again
                        repo.updateOutput(output);
                        messageId = completedId;
                    }
                }
            }
        } finally {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (IOException ignore) {
                }
            }
            try {
                handler.close();
            } catch (IOException e) {
                log.error("Error closing handler: " + e, e);
            }
        }
    }

    public void outputChanged(Output o) {
        // if o is the same object as our current Output instance then we made the change so don't stop
        if (o != output && thread != null) thread.interrupt();
    }

    public synchronized void stop() {
        stopFlag = true;
        if (thread != null) thread.interrupt();
    }
}
