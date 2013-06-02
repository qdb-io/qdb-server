package io.qdb.server.output;

import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.MessageCursor;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.model.Repository;
import io.qdb.server.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Watches a queue for new messages and processes them.
 */
public class OutputJob implements Runnable {

    protected static final Logger log = LoggerFactory.getLogger(OutputJob.class);

    protected final Repository repo;
    protected final QueueManager queueManager;
    protected final String oid;
    protected final OutputHandler handler;

    @Inject
    public OutputJob(Repository repo, QueueManager queueManager, String oid, OutputHandler handler) {
        this.repo = repo;
        this.queueManager = queueManager;
        this.oid = oid;
        this.handler = handler;
    }

    @Override
    public void run() {
        try {
            processMessages();
        } catch (IOException e) {
            log.error(e.toString(), e);
        }
    }

    /**
     * Feed messages to our handler until we are closed or our output is changed by someone else.
     */
    private void processMessages() throws IOException {
        // the output and/or queue might have been deleted
        Output output = repo.findOutput(oid);
        if (output == null) {
            if (log.isDebugEnabled()) log.debug("Output [" + oid + "] does not exist");
            return;
        }
        Queue q = repo.findQueue(output.getQueue());
        if (q == null) {
            if (log.isDebugEnabled()) log.debug("Queue [" + output.getQueue() + "] does not exist");
            return;
        }

        MessageBuffer buffer = queueManager.getBuffer(q);
        if (buffer == null) {   // we might be busy starting up or something
            if (log.isDebugEnabled()) log.debug("Queue [" + q.getId() + "[ does not have a buffer");
            return;
        }

        MessageCursor cursor = null;
        try {
            handler.init(q, output);

            long messageId = output.getMessageId();
            if (messageId < 0) {
                cursor = buffer.cursorByTimestamp(output.getTimestamp());
            } else {
                cursor = buffer.cursor(messageId == 0 ? buffer.getNextMessageId() : messageId);
            }

            long completedId = messageId;
            long timestamp = 0;
            long lastUpdate = System.currentTimeMillis();
            int updateIntervalMs = output.getUpdateIntervalMs();
            boolean stopFlag = false;
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

                Output o = repo.findOutput(oid);
                if (o != output) stopFlag = true; // output has been changed by someone else

                if (completedId != messageId && (stopFlag || updateIntervalMs <= 0
                        || System.currentTimeMillis() - lastUpdate >= updateIntervalMs)) {
                    synchronized (repo) {
                        o = repo.findOutput(oid);
                        // don't record our progress if we are now supposed to be processing from a different point in buffer
                        if (o.getMessageId() != output.getMessageId() || o.getTimestamp() != output.getTimestamp()) break;
                        output = (Output)o.clone();
                        output.setMessageId(completedId);
                        output.setTimestamp(timestamp);
                        handler.updateOutput(output);
                        repo.updateOutput(output);
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
}
