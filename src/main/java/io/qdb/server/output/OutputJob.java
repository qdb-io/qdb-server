package io.qdb.server.output;

import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.MessageCursor;
import io.qdb.server.databind.DataBinder;
import io.qdb.server.model.Database;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.queue.QueueManager;
import io.qdb.server.repo.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Watches a queue for new messages and processes them.
 */
public class OutputJob implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OutputJob.class);

    private final OutputManager outputManager;
    private final OutputHandlerFactory handlerFactory;
    private final QueueManager queueManager;
    private final Repository repo;
    private final String oid;

    private Thread thread;
    private String outputPath;
    private Output output;
    private int errorCount;
    private boolean stopFlag;

    public OutputJob(OutputManager outputManager, OutputHandlerFactory handlerFactory, QueueManager queueManager,
                Repository repo, String oid) {
        this.outputManager = outputManager;
        this.handlerFactory = handlerFactory;
        this.queueManager = queueManager;
        this.repo = repo;
        this.oid = oid;
    }

    public String getOid() {
        return oid;
    }

    @Override
    public void run() {
        thread = Thread.currentThread();
        try {
            mainLoop();
        } catch (Exception x) {
            log.error(this + ": " + x, x);
        } finally {
            if (log.isDebugEnabled()) log.debug(this + " exit");
            outputManager.onOutputJobExit(this);
        }
    }

    private void mainLoop() throws Exception {
        while (!isStopFlag()) {

            output = repo.findOutput(oid);
            if (output == null) {
                if (log.isDebugEnabled()) log.debug("Output [" + oid + "] does not exist");
                return;
            }
            if (!output.isEnabled()) return;

            Queue q = repo.findQueue(output.getQueue());
            if (q == null) {
                if (log.isDebugEnabled()) log.debug("Queue [" + output.getQueue() + "] does not exist");
                return;
            }

            Database db = repo.findDatabase(q.getDatabase());
            if (db == null) {
                if (log.isDebugEnabled()) log.debug("Database [" + q.getDatabase() + "] does not exist");
                return;
            }

            outputPath = toPath(db, q, output);

            OutputHandler handler;
            try {
                handler = handlerFactory.createHandler(output.getType());
            } catch (IllegalArgumentException e) {
                log.error("Error creating handler for " + outputPath + ": " + e.getMessage(), e);
                return;
            }

            boolean initOk = false;
            try {
                Map<String, Object> p = output.getParams();
                if (p != null) new DataBinder().ignoreInvalidFields(true).bind(p, handler).check();
                handler.init(q, output, outputPath);
                initOk = true;
            } catch (IllegalArgumentException e) {
                log.error(outputPath + ": " + e.getMessage());
                return;
            } catch (Exception e) {
                log.error(outputPath + ": " + e.getMessage(), e);
                ++errorCount;
            }

            try {
                if (initOk) {
                    MessageBuffer buffer = queueManager.getBuffer(q);
                    if (buffer == null) {   // we might be busy starting up or something
                        if (log.isDebugEnabled()) log.debug("Queue [" + q.getId() + "] does not have a buffer");
                        ++errorCount;
                    } else {
                        try {
                            processMessages(buffer, handler);
                        } catch (Exception e) {
                            ++errorCount;
                            log.error(outputPath + ": " + e.getMessage(), e);
                        }
                    }
                }
            } finally {
                try {
                    handler.close();
                } catch (IOException e) {
                    log.error(outputPath + ": Error closing handler: " + e, e);
                }
            }

            // todo use backoff policy from output
            int sleepMs = errorCount * 1000;
            if (sleepMs > 0) {
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    /**
     * Feed messages to our handler until we are closed or our output is changed by someone else.
     */
    public void processMessages(MessageBuffer buffer, OutputHandler handler) throws Exception {
        if (log.isDebugEnabled()) log.debug(outputPath + ": processing messages");
        MessageCursor cursor = null;
        try {
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

            boolean exitLoop = false;
            while (!exitLoop && !isStopFlag()) {
                boolean haveMsg;
                try {
                    haveMsg = cursor.next(1000);
                } catch (IOException e) {
                    haveMsg = false;
                    exitLoop = true;
                    log.error(outputPath + ": " + e, e);
                } catch (InterruptedException e) {
                    haveMsg = false;
                    exitLoop = true;
                }
                if (haveMsg) {
                    try {
                        completedId = handler.processMessage(cursor.getId(), cursor.getRoutingKey(),
                                timestamp = cursor.getTimestamp(), cursor.getPayload());
                        errorCount = 0; // we successfully processed a message
                    } catch (Exception e) {
                        exitLoop = true;
                        ++errorCount;
                        log.error(outputPath + ": " + e, e);
                    }
                }

                Output o = repo.findOutput(oid);
                if (o != output) {
                    exitLoop = true; // output has been changed by someone else
                    errorCount = 0;
                }

                if (completedId != messageId && (exitLoop || updateIntervalMs <= 0
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
                } catch (IOException e) {
                    log.error(outputPath + ": Error closing cursor: " + e);
                }
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

    private synchronized boolean isStopFlag() {
        return stopFlag;
    }

    /**
     * Create user friendly identifier for the output for error messages and so on.
     */
    private String toPath(Database db, Queue q, Output o) {
        StringBuilder b = new StringBuilder();
        b.append("/databases/").append(db.getId());
        String s = db.getQueueForQid(q.getId());
        if (s != null) {
            b.append("/queues/").append(s);
            s = q.getOutputForOid(o.getId());
            if (s != null) {
                return b.append("/outputs/").append(s).toString();
            }
        }
        return o.toString();
    }

    @Override
    public String toString() {
        return outputPath == null ? "output[" + oid + "]" : outputPath;
    }
}
